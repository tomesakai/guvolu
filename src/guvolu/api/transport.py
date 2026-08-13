"""HTTP 传输层：限速、签名、业务校验、写留痕。

职责边界：本层不理解业务模型，只负责把请求可靠送达并
返回 data 字段；T-10 的 status 校验与 R-07 的写留痕在此完成。
"""
from __future__ import annotations

import json
import random
import threading
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import requests

from guvolu.api.signing import sign_request
from guvolu.domain.errors import (
    ApiHttpError,
    ApiNetworkError,
    ApiTimeout,
    GmoApiError,
)
from guvolu.domain.ids import new_correlation_id

PUBLIC_BASE_URL = "https://api.coin.z.com/public"
PRIVATE_BASE_URL = "https://api.coin.z.com/private"

_TIMEOUT_SECONDS = 15.0
# GET 重试上限（C-08）
_GET_RETRY_MAX = 3
_GET_BACKOFF_BASE_SECONDS = 0.5

HttpMethod = Literal["GET", "POST", "PUT", "DELETE"]

Params = Mapping[str, str | int]


class RateLimiter:
    """进程内限速器（R-04），线程安全。"""

    def __init__(self, rate_per_second: float) -> None:
        if rate_per_second <= 0:
            raise ValueError("限速速率必须为正")
        self._interval = 1.0 / rate_per_second
        self._lock = threading.Lock()
        self._next_at = 0.0

    def acquire(self) -> None:
        """必要时阻塞等待到下一时隙。"""
        with self._lock:
            now = time.monotonic()
            wait = self._next_at - now
            self._next_at = max(now, self._next_at) + self._interval
        if wait > 0:
            time.sleep(wait)


def _extract_data(payload: Mapping[str, object], path: str, http_status: int) -> object:
    """T-10：校验 status 并取出 data。"""
    if payload.get("status") != 0:
        raw_messages = payload.get("messages")
        rows = raw_messages if isinstance(raw_messages, list) else []
        codes: list[str] = []
        texts: list[str] = []
        for row in rows:
            if isinstance(row, Mapping):
                codes.append(str(row.get("message_code", "")))
                texts.append(str(row.get("message_string", "")))
        raise GmoApiError(
            codes=tuple(codes),
            messages=tuple(texts),
            path=path,
            http_status=http_status,
        )
    return payload.get("data")


# 凭据脱敏占位（T-01）
_REDACTED = "***"


def redact_body(body: Mapping[str, object] | None) -> dict[str, object] | None:
    """脱敏请求体。令牌为承载凭据，不得入日志（T-01）。"""
    if body is None:
        return None
    cleaned = dict(body)
    if "token" in cleaned:
        cleaned["token"] = _REDACTED
    return cleaned


def redact_payload(
    path: str, payload: Mapping[str, object] | None
) -> dict[str, object] | None:
    """脱敏响应载荷。ws-auth 签发的 data 即令牌（T-01）。"""
    if payload is None:
        return None
    cleaned = dict(payload)
    if path == "/v1/ws-auth" and "data" in cleaned:
        cleaned["data"] = _REDACTED
    return cleaned


def _parse_json(response: requests.Response, path: str) -> Mapping[str, object]:
    """解析 JSON，非 JSON 视为 HTTP 层错误。"""
    try:
        payload = response.json()
    except ValueError as exc:
        raise ApiHttpError(response.status_code, path, "非 JSON 响应") from exc
    if not isinstance(payload, Mapping):
        raise ApiHttpError(response.status_code, path, "响应结构非对象")
    return payload


class PublicTransport:
    """公开 API 传输，无需密钥。"""

    def __init__(
        self, limiter: RateLimiter, base_url: str = PUBLIC_BASE_URL
    ) -> None:
        self._limiter = limiter
        self._base_url = base_url
        self._session = requests.Session()

    def get_payload(
        self, path: str, params: Params | None = None
    ) -> Mapping[str, object]:
        """取完整载荷，含 responsetime，供时钟校验。"""
        last_error: Exception | None = None
        for attempt in range(_GET_RETRY_MAX):
            self._limiter.acquire()
            try:
                response = self._session.get(
                    self._base_url + path,
                    params=dict(params) if params else None,
                    timeout=_TIMEOUT_SECONDS,
                )
            except requests.Timeout as exc:
                last_error = ApiTimeout(path, str(exc))
            except requests.RequestException as exc:
                last_error = ApiNetworkError(path, str(exc))
            else:
                if response.status_code >= 500:
                    last_error = ApiHttpError(response.status_code, path)
                else:
                    return _parse_json(response, path)
            _sleep_backoff(attempt)
        assert last_error is not None
        raise last_error

    def get(self, path: str, params: Params | None = None) -> object:
        """GET 并返回 data。频率超限按错误处置册退避重试。"""
        last_error: GmoApiError | None = None
        for attempt in range(_GET_RETRY_MAX):
            payload = self.get_payload(path, params)
            try:
                return _extract_data(payload, path, 200)
            except GmoApiError as exc:
                if "ERR-5003" not in exc.codes:
                    raise
                last_error = exc
                _sleep_backoff(attempt)
        assert last_error is not None
        raise last_error


def _sleep_backoff(attempt: int) -> None:
    """指数退避加随机抖动（C-08）。"""
    delay = _GET_BACKOFF_BASE_SECONDS * (2**attempt)
    time.sleep(delay + random.uniform(0.0, delay / 2))


class PrivateTransport:
    """私有 API 传输，持单一密钥（T-02 由客户端类保证）。"""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        limiter: RateLimiter,
        log_dir: Path,
        base_url: str = PRIVATE_BASE_URL,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._limiter = limiter
        self._log_dir = log_dir
        self._base_url = base_url
        self._session = requests.Session()
        self._log_lock = threading.Lock()

    def request(
        self,
        method: HttpMethod,
        path: str,
        *,
        params: Params | None = None,
        body: Mapping[str, object] | None = None,
    ) -> object:
        """签名请求并返回 data。写请求永不重试（C-08）。"""
        if method == "GET":
            return self._get_with_retry(path, params)
        return self._write_once(method, path, body)

    def _headers(self, method: HttpMethod, path: str, body_text: str) -> dict[str, str]:
        timestamp_ms = str(int(time.time() * 1000))
        signature = sign_request(
            self._api_secret, timestamp_ms, method, path, body_text
        )
        headers = {
            "API-KEY": self._api_key,
            "API-TIMESTAMP": timestamp_ms,
            "API-SIGN": signature,
        }
        if body_text:
            headers["Content-Type"] = "application/json"
        return headers

    def _get_with_retry(self, path: str, params: Params | None) -> object:
        last_error: Exception | None = None
        for attempt in range(_GET_RETRY_MAX):
            self._limiter.acquire()
            try:
                response = self._session.get(
                    self._base_url + path,
                    params=dict(params) if params else None,
                    headers=self._headers("GET", path, ""),
                    timeout=_TIMEOUT_SECONDS,
                )
            except requests.Timeout as exc:
                last_error = ApiTimeout(path, str(exc))
            except requests.RequestException as exc:
                last_error = ApiNetworkError(path, str(exc))
            else:
                if response.status_code >= 500:
                    last_error = ApiHttpError(response.status_code, path)
                else:
                    payload = _parse_json(response, path)
                    try:
                        return _extract_data(
                            payload, path, response.status_code
                        )
                    except GmoApiError as exc:
                        # 频率超限退避重试，其余直抛
                        if "ERR-5003" not in exc.codes:
                            raise
                        last_error = exc
            _sleep_backoff(attempt)
        assert last_error is not None
        raise last_error

    def _write_once(
        self, method: HttpMethod, path: str, body: Mapping[str, object] | None
    ) -> object:
        body_text = json.dumps(body) if body is not None else ""
        correlation_id = new_correlation_id()
        self._limiter.acquire()
        try:
            response = self._session.request(
                method,
                self._base_url + path,
                data=body_text or None,
                headers=self._headers(method, path, body_text),
                timeout=_TIMEOUT_SECONDS,
            )
        except requests.Timeout as exc:
            self._log_write(correlation_id, method, path, body, None, None, str(exc))
            raise ApiTimeout(path, str(exc)) from exc
        except requests.RequestException as exc:
            self._log_write(correlation_id, method, path, body, None, None, str(exc))
            raise ApiNetworkError(path, str(exc)) from exc
        payload = _parse_json(response, path)
        self._log_write(
            correlation_id, method, path, body, response.status_code, payload, None
        )
        return _extract_data(payload, path, response.status_code)

    def _log_write(
        self,
        correlation_id: str,
        method: str,
        path: str,
        body: Mapping[str, object] | None,
        http_status: int | None,
        payload: Mapping[str, object] | None,
        error: str | None,
    ) -> None:
        """写操作全量留痕（R-07）。密钥绝不入日志（T-01）。"""
        entry = {
            "ts": datetime.now(UTC).isoformat(),
            "correlation_id": correlation_id,
            "method": method,
            "path": path,
            "body": redact_body(body),
            "http_status": http_status,
            "payload": redact_payload(path, payload),
            "error": error,
        }
        line = json.dumps(entry, ensure_ascii=False, default=str)
        day = datetime.now(UTC).strftime("%Y%m%d")
        log_path = self._log_dir / f"api-{day}.jsonl"
        with self._log_lock:
            self._log_dir.mkdir(parents=True, exist_ok=True)
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
