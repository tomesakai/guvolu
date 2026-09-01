"""进程配置装载（T-01、T-04、T-11）。"""
from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from guvolu.domain.enums import RunMode
from guvolu.domain.errors import ConfigError
from guvolu.domain.symbols import SpotSymbol

# 绝对硬顶（T-11），修改须重启进程
# 2026-09-01 维护者确认上调
MAX_ORDER_JPY_CEILING = Decimal("10000")
MAX_DAY_JPY_CEILING = Decimal("10000")
MAX_DAY_COUNT_CEILING = 50


def _load_env_file(path: Path) -> dict[str, str]:
    """解析 .env 文件为键值表。"""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"')
    return values


@dataclass(frozen=True, slots=True)
class Limits:
    """风控限额（T-11）。运行时只能调低，不得越过硬顶。"""

    order_jpy_max: Decimal
    day_jpy_max: Decimal
    day_count_max: int


@dataclass(frozen=True, slots=True)
class Config:
    """进程配置。密钥可缺省，按进程职责惰性索取（T-13）。"""

    mode: RunMode
    read_api_key: str | None
    read_api_secret: str | None
    trade_api_key: str | None
    trade_api_secret: str | None
    bitflyer_read_api_key: str | None
    bitflyer_read_api_secret: str | None
    bitflyer_private_rps: float
    spot_whitelist: frozenset[SpotSymbol]
    limits: Limits
    log_dir: Path
    private_rps: float
    public_rps: float
    # 近窗逐笔端点秒窗上限
    recent_trades_max_seconds: int
    # 当日瓦片增量刷新间隔秒
    tile_refresh_seconds: int

    def require_read_credentials(self) -> tuple[str, str]:
        """取 READ_ONLY 密钥，缺失即配置错误。"""
        if not self.read_api_key or not self.read_api_secret:
            raise ConfigError("缺少 READ_ONLY 密钥")
        return self.read_api_key, self.read_api_secret

    def require_trade_credentials(self) -> tuple[str, str]:
        """取 TRADE 密钥，缺失即配置错误。"""
        if not self.trade_api_key or not self.trade_api_secret:
            raise ConfigError("缺少 TRADE 密钥")
        return self.trade_api_key, self.trade_api_secret

    def bitflyer_read_credentials(self) -> tuple[str, str] | None:
        """bitFlyer 读取凭据成对存在时返回；缺省即不接入账户查询。"""
        if not self.bitflyer_read_api_key or not self.bitflyer_read_api_secret:
            return None
        return self.bitflyer_read_api_key, self.bitflyer_read_api_secret


def load_config(env_file: Path | None = None) -> Config:
    """装载配置。进程环境变量优先于 .env 文件。"""
    file_values = _load_env_file(env_file if env_file is not None else Path(".env"))

    def get(name: str, default: str = "") -> str:
        return os.environ.get(name, file_values.get(name, default))

    mode_raw = get("GUVOLU_MODE", RunMode.DRY_RUN.value)
    try:
        mode = RunMode(mode_raw)
    except ValueError as exc:
        raise ConfigError(f"非法运行模式: {mode_raw!r}") from exc

    whitelist = frozenset(
        SpotSymbol(part.strip())
        for part in get("GUVOLU_SPOT_WHITELIST", "BTC").split(",")
        if part.strip()
    )
    limits = Limits(
        order_jpy_max=min(
            Decimal(get("GUVOLU_ORDER_JPY_MAX", "10000")),
            MAX_ORDER_JPY_CEILING,
        ),
        day_jpy_max=min(
            Decimal(get("GUVOLU_DAY_JPY_MAX", "10000")), MAX_DAY_JPY_CEILING
        ),
        day_count_max=min(
            int(get("GUVOLU_DAY_COUNT_MAX", "48")), MAX_DAY_COUNT_CEILING
        ),
    )
    return Config(
        mode=mode,
        read_api_key=get("GMO_COIN_READ_ONLY_API_KEY") or None,
        read_api_secret=get("GMO_COIN_READ_ONLY_API_SECRET") or None,
        trade_api_key=get("GMO_COIN_TRADE_API_KEY") or None,
        trade_api_secret=get("GMO_COIN_TRADE_API_SECRET") or None,
        bitflyer_read_api_key=get("BITFLYER_READ_ONLY_API_KEY") or None,
        bitflyer_read_api_secret=get("BITFLYER_READ_ONLY_API_SECRET") or None,
        bitflyer_private_rps=float(get("GUVOLU_BITFLYER_PRIVATE_RPS", "1.5")),
        spot_whitelist=whitelist,
        limits=limits,
        log_dir=Path(get("GUVOLU_LOG_DIR", "logs")),
        private_rps=float(get("GUVOLU_PRIVATE_RPS", "10")),
        public_rps=float(get("GUVOLU_PUBLIC_RPS", "3")),
        recent_trades_max_seconds=int(
            get("GUVOLU_RECENT_TRADES_MAX_SECONDS", "600")
        ),
        tile_refresh_seconds=int(get("GUVOLU_TILE_REFRESH_SECONDS", "15")),
    )
