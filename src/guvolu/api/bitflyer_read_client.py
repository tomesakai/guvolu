"""bitFlyer 账户读取客户端。

只接受 READ_ONLY 凭据，账户事实不与公开行情适配器混用。
"""
from __future__ import annotations

import hashlib
import hmac
import random
import time
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

import requests

from guvolu.api.transport import RateLimiter
from guvolu.domain.config import Config
from guvolu.domain.errors import ApiHttpError, ApiNetworkError, ApiSchemaError, ApiTimeout

BASE_URL = "https://api.bitflyer.com"
GET_BALANCE_PATH = "/v1/me/getbalance"
TIMEOUT_SECONDS = 15.0
PRIVATE_RPS = 1.5
GET_RETRY_MAX = 3
GET_BACKOFF_BASE_SECONDS = 0.5


@dataclass(frozen=True, slots=True)
class BitflyerAsset:
    """bitFlyer 资产原文的金额字段，均保持 Decimal。"""

    symbol: str
    amount: Decimal
    available: Decimal

    @classmethod
    def from_api(cls, row: Mapping[str, object]) -> "BitflyerAsset":
        try:
            return cls(
                symbol=str(row["currency_code"]),
                amount=Decimal(str(row["amount"])),
                available=Decimal(str(row["available"])),
            )
        except (KeyError, InvalidOperation, ValueError) as exc:
            raise ApiSchemaError("bitFlyer getbalance 字段非法") from exc


class BitflyerReadClient:
    """只读账户客户端；不暴露任何交易、出金或划转方法。"""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        limiter: RateLimiter | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._limiter = limiter if limiter is not None else RateLimiter(PRIVATE_RPS)
        self._session = session if session is not None else requests.Session()

    @classmethod
    def from_config(cls, config: Config) -> "BitflyerReadClient | None":
        """缺少成对读取凭据时，账户来源明确未配置而非请求失败。"""
        credentials = config.bitflyer_read_credentials()
        if credentials is None:
            return None
        return cls(*credentials, limiter=RateLimiter(config.bitflyer_private_rps))

    def assets(self) -> tuple[BitflyerAsset, ...]:
        """读取账户资产；GET 失败不退化为零值。"""
        last_error: ApiNetworkError | ApiHttpError | None = None
        response: requests.Response | None = None
        for attempt in range(GET_RETRY_MAX):
            timestamp = str(int(time.time()))
            signed = timestamp + "GET" + GET_BALANCE_PATH
            signature = hmac.new(
                self._api_secret.encode("utf-8"),
                signed.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            self._limiter.acquire()
            try:
                response = self._session.get(
                    BASE_URL + GET_BALANCE_PATH,
                    headers={
                        "ACCESS-KEY": self._api_key,
                        "ACCESS-TIMESTAMP": timestamp,
                        "ACCESS-SIGN": signature,
                    },
                    timeout=TIMEOUT_SECONDS,
                )
            except requests.Timeout as exc:
                last_error = ApiTimeout(GET_BALANCE_PATH, str(exc))
            except requests.RequestException as exc:
                last_error = ApiNetworkError(GET_BALANCE_PATH, str(exc))
            else:
                if response.status_code == 200:
                    break
                error = ApiHttpError(response.status_code, GET_BALANCE_PATH)
                # 429 属可恢复限速，同 5xx 退避
                if response.status_code != 429 and response.status_code < 500:
                    raise error
                last_error = error
            if attempt + 1 < GET_RETRY_MAX:
                delay = GET_BACKOFF_BASE_SECONDS * (2**attempt)
                time.sleep(delay + random.uniform(0, delay))
        if response is None or response.status_code != 200:
            assert last_error is not None
            raise last_error
        try:
            payload = response.json()
        except ValueError as exc:
            raise ApiSchemaError("bitFlyer getbalance 非 JSON 响应") from exc
        if not isinstance(payload, list):
            raise ApiSchemaError("bitFlyer getbalance 响应非数组")
        assets: list[BitflyerAsset] = []
        for row in payload:
            if not isinstance(row, Mapping):
                raise ApiSchemaError("bitFlyer getbalance 行非对象")
            assets.append(BitflyerAsset.from_api(row))
        return tuple(assets)
