"""OKX 历史 L2 计划、断点下载与不可变封口。"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import BinaryIO, cast
from urllib.parse import urlparse

import requests

from guvolu.data.durable_io import (
    atomic_write_bytes,
    atomic_write_text,
    exclusive_path_lock,
)
from guvolu.data.materialize import artifact_id, sha256_file, utc_now
from guvolu.data.paths import data_root as configured_data_root

OKX_DOWNLOAD_PLAN_URL = (
    "https://www.okx.com/priapi/v5/broker/public/"
    "trade-data/download-link"
)
OKX_INSTRUMENT_URL = "https://www.okx.com/api/v5/public/instruments"
OKX_HISTORY_PAGE = "https://www.okx.com/historical-data"
OKX_ARCHIVE_HOST = "static.okx.com"
OKX_BOOK_HISTORY_ENDPOINT = "historical-data/order-book"
OKX_PLAN_MODULE = {400: "4", 5000: "5"}
CHECKPOINT_BYTES = 16 * 1024 * 1024
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
ARCHIVE_SCHEMA_VERSION = 1
_SYMBOL = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")
_FILENAME = re.compile(r"^[A-Za-z0-9_.-]+\.tar\.gz$")


@dataclass(frozen=True, slots=True)
class OkxArchivePlan:
    """一份经官方计划接口返回的日级 L2 文件。"""

    venue_symbol: str
    day: str
    depth_levels: int
    filename: str
    url: str
    advertised_size_mb: str
    response_artifact_id: str
    response_body: bytes


@dataclass(frozen=True, slots=True)
class OkxDownloadResult:
    """封口归档及其证据。"""

    artifact_id: str
    storage_path: str
    manifest_path: str
    byte_count: int
    sha256: str
    reused: bool


def _validate_symbol(venue_symbol: str) -> str:
    if _SYMBOL.fullmatch(venue_symbol) is None:
        raise ValueError(f"OKX 品种格式非法: {venue_symbol!r}")
    return venue_symbol


def _day_end_millis(day: date) -> str:
    """按下载页面所在东京时区生成所选日末毫秒。"""
    local = datetime.combine(day, time.max, timezone(timedelta(hours=9)))
    return str(int(local.timestamp() * 1000))


def _json_object(body: bytes) -> Mapping[str, object]:
    try:
        loaded = json.loads(body)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("OKX 返回不是有效 JSON") from exc
    if not isinstance(loaded, Mapping):
        raise ValueError("OKX 返回顶层不是对象")
    return loaded


def parse_download_plan(
    response_body: bytes,
    *,
    venue_symbol: str,
    day: date,
    depth_levels: int,
) -> OkxArchivePlan:
    """把原始计划响应收窄为一份严格匹配的归档。"""
    _validate_symbol(venue_symbol)
    if depth_levels not in OKX_PLAN_MODULE:
        raise ValueError("OKX 历史 L2 深度只允许 400 或 5000")
    payload = _json_object(response_body)
    if str(payload.get("code")) != "0":
        raise ValueError(f"OKX 计划失败: {payload.get('code')} {payload.get('msg')}")
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise ValueError("OKX 计划缺少 data")
    details = data.get("details")
    if not isinstance(details, list):
        raise ValueError("OKX 计划缺少 details")
    expected_day = day.isoformat()
    expected_token = day.strftime("%Y-%m-%d")
    matches: list[Mapping[str, object]] = []
    for detail in details:
        if not isinstance(detail, Mapping):
            continue
        if str(detail.get("instId")) != venue_symbol:
            continue
        groups = detail.get("groupDetails")
        if not isinstance(groups, list):
            continue
        for group in groups:
            if not isinstance(group, Mapping):
                continue
            if expected_token in str(group.get("filename", "")):
                matches.append(group)
    if len(matches) != 1:
        raise ValueError(
            f"OKX 计划应唯一命中 {venue_symbol}/{expected_day}，实际 {len(matches)}"
        )
    match = matches[0]
    filename = str(match.get("filename", ""))
    url = str(match.get("url", ""))
    if _FILENAME.fullmatch(filename) is None:
        raise ValueError(f"OKX 文件名非法: {filename!r}")
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != OKX_ARCHIVE_HOST:
        raise ValueError(f"OKX 下载域名非法: {url!r}")
    if not parsed.path.endswith("/" + filename):
        raise ValueError("OKX 下载 URL 与文件名不一致")
    depth_token = f"-{depth_levels}lv-"
    if depth_token not in filename:
        raise ValueError("OKX 下载文件深度与请求不一致")
    response_sha = hashlib.sha256(response_body).hexdigest()
    return OkxArchivePlan(
        venue_symbol=venue_symbol,
        day=expected_day,
        depth_levels=depth_levels,
        filename=filename,
        url=url,
        advertised_size_mb=str(match.get("sizeMB", "")),
        response_artifact_id=artifact_id(response_sha),
        response_body=response_body,
    )


def request_download_plan(
    session: requests.Session,
    *,
    venue_symbol: str,
    day: date,
    depth_levels: int = 400,
    timeout_seconds: float = 30.0,
) -> OkxArchivePlan:
    """调用页面公开计划接口；不触碰交易端点。"""
    _validate_symbol(venue_symbol)
    module = OKX_PLAN_MODULE.get(depth_levels)
    if module is None:
        raise ValueError("OKX 历史 L2 深度只允许 400 或 5000")
    day_end = _day_end_millis(day)
    response = session.post(
        OKX_DOWNLOAD_PLAN_URL,
        json={
            "module": module,
            "instType": "SPOT",
            "instQueryParam": {"instIdList": [venue_symbol]},
            "dateQuery": {
                "dateAggrType": "daily",
                "begin": day_end,
                "end": day_end,
            },
        },
        headers={"Origin": "https://www.okx.com", "Referer": OKX_HISTORY_PAGE},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return parse_download_plan(
        response.content,
        venue_symbol=venue_symbol,
        day=day,
        depth_levels=depth_levels,
    )


def request_instrument(
    session: requests.Session,
    *,
    venue_symbol: str,
    timeout_seconds: float = 30.0,
) -> bytes:
    """取得现行品种规则原文，并验证请求成功。"""
    _validate_symbol(venue_symbol)
    response = session.get(
        OKX_INSTRUMENT_URL,
        params={"instType": "SPOT", "instId": venue_symbol},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    body = response.content
    payload = _json_object(body)
    rows = payload.get("data")
    if str(payload.get("code")) != "0" or not isinstance(rows, list):
        raise ValueError("OKX 品种规则请求失败")
    matches = [
        row for row in rows
        if isinstance(row, Mapping) and row.get("instId") == venue_symbol
    ]
    if len(matches) != 1:
        raise ValueError("OKX 品种规则应唯一命中")
    return body


def _archive_directory(root: Path, plan: OkxArchivePlan) -> Path:
    return (
        root / "raw" / "archive" / "okx" / "book_l2"
        / f"venue_symbol={plan.venue_symbol}"
        / f"day={plan.day}"
    )


def _persist_evidence(directory: Path, kind: str, body: bytes) -> tuple[str, str]:
    sha = hashlib.sha256(body).hexdigest()
    identity = artifact_id(sha)
    path = directory / "evidence" / f"{kind}-{sha[:16]}.json"
    if path.exists():
        if sha256_file(path) != sha:
            raise ValueError(f"OKX 证据散列冲突: {path}")
    else:
        atomic_write_bytes(path, body)
    return identity, path.as_posix()


def _checkpoint(
    path: Path,
    *,
    plan: OkxArchivePlan,
    downloaded_bytes: int,
    expected_bytes: int,
    response_etag: str | None,
    response_last_modified: str | None,
) -> None:
    atomic_write_text(path, json.dumps({
        "schema_version": ARCHIVE_SCHEMA_VERSION,
        "status": "open",
        "venue_id": "okx",
        "venue_symbol": plan.venue_symbol,
        "domain": "book_l2",
        "endpoint": OKX_BOOK_HISTORY_ENDPOINT,
        "day": plan.day,
        "depth_levels": plan.depth_levels,
        "downloaded_bytes": downloaded_bytes,
        "expected_bytes": expected_bytes,
        "response_etag": response_etag,
        "response_last_modified": response_last_modified,
        "source_url": plan.url,
        "updated_at": utc_now(),
    }, ensure_ascii=False, indent=2) + "\n")


def _response_total(response: requests.Response, offset: int) -> int:
    if response.status_code == 206:
        content_range = response.headers.get("Content-Range", "")
        match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", content_range)
        if match is None or int(match.group(1)) != offset:
            raise ValueError(f"OKX Range 响应非法: {content_range!r}")
        return int(match.group(3))
    length = response.headers.get("Content-Length")
    if response.status_code != 200 or length is None:
        raise ValueError("OKX 下载响应缺少可验证长度")
    return int(length)


def _write_stream(
    handle: BinaryIO,
    response: requests.Response,
    *,
    initial_bytes: int,
    expected_bytes: int,
    checkpoint_path: Path,
    plan: OkxArchivePlan,
) -> int:
    downloaded = initial_bytes
    checkpoint_at = downloaded
    etag = response.headers.get("ETag")
    last_modified = response.headers.get("Last-Modified")
    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
        if not chunk:
            continue
        handle.write(chunk)
        downloaded += len(chunk)
        if downloaded - checkpoint_at >= CHECKPOINT_BYTES:
            handle.flush()
            os.fsync(handle.fileno())
            _checkpoint(
                checkpoint_path,
                plan=plan,
                downloaded_bytes=downloaded,
                expected_bytes=expected_bytes,
                response_etag=etag,
                response_last_modified=last_modified,
            )
            checkpoint_at = downloaded
    handle.flush()
    os.fsync(handle.fileno())
    _checkpoint(
        checkpoint_path,
        plan=plan,
        downloaded_bytes=downloaded,
        expected_bytes=expected_bytes,
        response_etag=etag,
        response_last_modified=last_modified,
    )
    return downloaded


def _reuse_result(root: Path, archive: Path, manifest: Path) -> OkxDownloadResult:
    if not archive.is_file() or not manifest.is_file():
        raise FileNotFoundError("OKX 已封口归档或 manifest 缺失")
    body = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(body, Mapping):
        raise ValueError("OKX manifest 顶层不是对象")
    sha = sha256_file(archive)
    if (
        body.get("status") != "sealed"
        or body.get("completion_claim") is not True
        or str(body.get("sha256")) != sha
        or int(str(body.get("byte_count"))) != archive.stat().st_size
    ):
        raise ValueError("OKX 已封口归档与 manifest 不一致")
    return OkxDownloadResult(
        artifact_id=artifact_id(sha),
        storage_path=archive.relative_to(root).as_posix(),
        manifest_path=manifest.relative_to(root).as_posix(),
        byte_count=archive.stat().st_size,
        sha256=sha,
        reused=True,
    )


def _checkpoint_body(path: Path) -> Mapping[str, object] | None:
    if not path.is_file():
        return None
    body = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(body, Mapping):
        raise ValueError("OKX checkpoint 顶层不是对象")
    return body


def _manifest_body(
    root: Path,
    archive: Path,
    plan: OkxArchivePlan,
    *,
    sha: str,
    plan_identity: str,
    plan_evidence: str,
    instrument_identity: str,
    instrument_evidence: str,
    response_etag: str | None,
    response_last_modified: str | None,
) -> dict[str, object]:
    return {
        "schema_version": ARCHIVE_SCHEMA_VERSION,
        "status": "sealed",
        "completion_claim": True,
        "artifact_id": artifact_id(sha),
        "sha256": sha,
        "byte_count": archive.stat().st_size,
        "venue_id": "okx",
        "venue_symbol": plan.venue_symbol,
        "domain": "book_l2",
        "endpoint": OKX_BOOK_HISTORY_ENDPOINT,
        "day": plan.day,
        "depth_levels": plan.depth_levels,
        "archive_kind": "tar_gzip",
        "source_url": plan.url,
        "source_etag": response_etag,
        "source_last_modified": response_last_modified,
        "download_plan_artifact_id": plan_identity,
        "download_plan_path": Path(plan_evidence).relative_to(root).as_posix(),
        "instrument_artifact_id": instrument_identity,
        "instrument_path": Path(instrument_evidence).relative_to(root).as_posix(),
        "verification_method": "content-length+sha256-file-v1",
        "sealed_at": utc_now(),
        "storage_path": archive.relative_to(root).as_posix(),
    }


def _recover_unmanifested_archive(
    root: Path,
    archive: Path,
    partial: Path,
    checkpoint_path: Path,
    manifest: Path,
    plan: OkxArchivePlan,
    *,
    plan_identity: str,
    plan_evidence: str,
    instrument_identity: str,
    instrument_evidence: str,
) -> OkxDownloadResult:
    """只收口本下载器已校验并改名、但 manifest 未落盘的狭窄状态。"""
    if partial.exists():
        raise ValueError("OKX 封口恢复时同时存在 archive 与 .part")
    checkpoint = _checkpoint_body(checkpoint_path)
    if checkpoint is None:
        raise ValueError("OKX 孤立 archive 缺少 checkpoint，不自动采纳")
    expected = {
        "status": "open",
        "venue_id": "okx",
        "venue_symbol": plan.venue_symbol,
        "domain": "book_l2",
        "endpoint": OKX_BOOK_HISTORY_ENDPOINT,
        "day": plan.day,
        "depth_levels": plan.depth_levels,
        "source_url": plan.url,
    }
    if any(checkpoint.get(key) != value for key, value in expected.items()):
        raise ValueError("OKX 孤立 archive 与 checkpoint 身份不一致")
    size = archive.stat().st_size
    if (
        int(str(checkpoint.get("downloaded_bytes", -1))) != size
        or int(str(checkpoint.get("expected_bytes", -1))) != size
    ):
        raise ValueError("OKX 孤立 archive 长度未达到 checkpoint 完成态")
    sha = sha256_file(archive)
    atomic_write_text(
        manifest,
        json.dumps(_manifest_body(
            root,
            archive,
            plan,
            sha=sha,
            plan_identity=plan_identity,
            plan_evidence=plan_evidence,
            instrument_identity=instrument_identity,
            instrument_evidence=instrument_evidence,
            response_etag=cast(str | None, checkpoint.get("response_etag")),
            response_last_modified=cast(
                str | None, checkpoint.get("response_last_modified")
            ),
        ), ensure_ascii=False, indent=2) + "\n",
    )
    checkpoint_path.unlink()
    return _reuse_result(root, archive, manifest)


def download_archive(
    session: requests.Session,
    root: Path,
    plan: OkxArchivePlan,
    *,
    instrument_body: bytes,
    timeout_seconds: float = 120.0,
) -> OkxDownloadResult:
    """断点下载一份计划文件，校验长度和 SHA 后原子封口。"""
    root = root.resolve()
    directory = _archive_directory(root, plan)
    directory.mkdir(parents=True, exist_ok=True)
    archive = directory / plan.filename
    partial = directory / (plan.filename + ".part")
    checkpoint_path = directory / (plan.filename + ".checkpoint.json")
    manifest = directory / (plan.filename + ".manifest.json")
    with exclusive_path_lock(archive):
        if manifest.exists():
            return _reuse_result(root, archive, manifest)
        plan_identity, plan_evidence = _persist_evidence(
            directory, "download-plan", plan.response_body
        )
        instrument_identity, instrument_evidence = _persist_evidence(
            directory, "instrument", instrument_body
        )
        if archive.exists():
            return _recover_unmanifested_archive(
                root, archive, partial, checkpoint_path, manifest, plan,
                plan_identity=plan_identity,
                plan_evidence=plan_evidence,
                instrument_identity=instrument_identity,
                instrument_evidence=instrument_evidence,
            )
        offset = partial.stat().st_size if partial.exists() else 0
        checkpoint = _checkpoint_body(checkpoint_path)
        headers = {"Range": f"bytes={offset}-"} if offset else {}
        if offset and checkpoint and checkpoint.get("response_etag"):
            headers["If-Range"] = str(checkpoint["response_etag"])
        response = session.get(
            plan.url, headers=headers, stream=True, timeout=timeout_seconds
        )
        response.raise_for_status()
        if offset and response.status_code != 206:
            raise ValueError(
                "OKX CDN 未接受 Range；保留 .part，不静默重头覆盖"
            )
        expected_bytes = _response_total(response, offset)
        mode = "ab" if offset else "wb"
        with partial.open(mode) as handle:
            downloaded = _write_stream(
                cast(BinaryIO, handle),
                response,
                initial_bytes=offset,
                expected_bytes=expected_bytes,
                checkpoint_path=checkpoint_path,
                plan=plan,
            )
        if downloaded != expected_bytes or partial.stat().st_size != expected_bytes:
            raise ValueError(
                f"OKX 下载长度不符: {downloaded}/{expected_bytes}"
            )
        sha = sha256_file(partial)
        os.replace(partial, archive)
        atomic_write_text(
            manifest,
            json.dumps(_manifest_body(
                root,
                archive,
                plan,
                sha=sha,
                plan_identity=plan_identity,
                plan_evidence=plan_evidence,
                instrument_identity=instrument_identity,
                instrument_evidence=instrument_evidence,
                response_etag=response.headers.get("ETag"),
                response_last_modified=response.headers.get("Last-Modified"),
            ), ensure_ascii=False, indent=2) + "\n",
        )
        checkpoint_path.unlink()
        return OkxDownloadResult(
            artifact_id=artifact_id(sha),
            storage_path=archive.relative_to(root).as_posix(),
            manifest_path=manifest.relative_to(root).as_posix(),
            byte_count=archive.stat().st_size,
            sha256=sha,
            reused=False,
        )


def probe_and_download(
    root: Path,
    *,
    venue_symbol: str,
    day: date,
    depth_levels: int = 400,
) -> OkxDownloadResult:
    """取得计划与品种规则后持久化一份历史 L2 原件。"""
    with requests.Session() as session:
        session.headers.update({"User-Agent": "guvolu-okx-l2-probe/1"})
        plan = request_download_plan(
            session,
            venue_symbol=venue_symbol,
            day=day,
            depth_levels=depth_levels,
        )
        instrument = request_instrument(session, venue_symbol=venue_symbol)
        return download_archive(
            session, root, plan, instrument_body=instrument
        )


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(description="OKX 历史 L2 小样本下载")
    parser.add_argument("--data-root", type=Path, default=None)
    parser.add_argument("--symbol", default="BTC-USDT")
    parser.add_argument("--day", type=date.fromisoformat, required=True)
    parser.add_argument("--depth", type=int, choices=sorted(OKX_PLAN_MODULE), default=400)
    args = parser.parse_args(argv)
    result = probe_and_download(
        (args.data_root or configured_data_root()).resolve(),
        venue_symbol=cast(str, args.symbol),
        day=cast(date, args.day),
        depth_levels=cast(int, args.depth),
    )
    print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
