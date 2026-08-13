"""紧急停止开关：全品种撤单的独立入口（T-07）。

本模块绝不导入策略、风控与控制面，也不依赖任何策略状态，
异常时可脱离主进程直接执行。撤单只减少风险，故不受模拟运行守卫限制。
"""
from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from guvolu.api.public_client import PublicClient
from guvolu.api.trade_client import TradeClient
from guvolu.domain.config import load_config
from guvolu.domain.errors import GmoApiError
from guvolu.domain.symbols import Symbol, parse_symbol


def collect_symbols(public: PublicClient) -> tuple[Symbol, ...]:
    """取全部品种。现物与杠杆一并撤单，按形态分类型（U-02）。"""
    return tuple(parse_symbol(rule.symbol) for rule in public.symbols())


def cancel_all(public: PublicClient, trade: TradeClient) -> int:
    """撤销全部品种的挂单，返回进程退出码。

    受理结果只证明请求被接受，实际状态以 READ_ONLY 为准（T-03）。
    """
    symbols = collect_symbols(public)
    try:
        order_ids = trade.cancel_bulk_order(symbols)
    except GmoApiError as error:
        codes = ",".join(error.codes) or "无错误码"
        print(f"全量撤单失败，错误码 {codes}: {error}")
        return 1
    listed = ",".join(str(order_id) for order_id in order_ids) or "无"
    print(f"品种 {len(symbols)} 个，受理撤单 {len(order_ids)} 笔: {listed}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。装载配置后对全部品种执行撤单。"""
    parser = argparse.ArgumentParser(description="紧急停止开关：全品种撤单")
    parser.add_argument(
        "--env-file", default=None, help="配置文件路径，缺省读取 .env"
    )
    args = parser.parse_args(argv)
    raw_env_file: str | None = args.env_file
    env_file = Path(raw_env_file) if raw_env_file is not None else None
    config = load_config(env_file)
    public = PublicClient.from_config(config)
    trade = TradeClient.from_config(config)
    return cancel_all(public, trade)


if __name__ == "__main__":
    raise SystemExit(main())
