"""授权信封签发前校验：装载合同、显示身份与关键额度。

供维护者在签署（覆盖 config/authorization_envelope.json）前
运行：按第 14 节合同完整校验字段集合与取值，打印 SHA-256、
有效期、各额度与熔断动作，并显示该身份对应的用量与状态文件
是否已存在。只读，不写任何文件，不打印任何密钥（A-06）。
"""
from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from guvolu.domain.config import load_config
from guvolu.execution.authorization_envelope import (
    DEFAULT_ENVELOPE_PATH,
    EnvelopeStateStore,
    EnvelopeUsage,
    load_envelope,
)


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。校验失败打印原因并返回非零。"""
    parser = argparse.ArgumentParser(
        description="授权信封签发前校验（只读）"
    )
    parser.add_argument(
        "--envelope", type=Path, default=DEFAULT_ENVELOPE_PATH,
        help="待校验信封路径，可指向草案文件",
    )
    parser.add_argument("--env-file", type=Path, default=None)
    args = parser.parse_args(argv)
    env_file: Path | None = args.env_file
    config = load_config(env_file)
    envelope = load_envelope(
        Path(args.envelope), whitelist=config.spot_whitelist
    )
    usage = EnvelopeUsage.for_envelope(envelope)
    state_store = EnvelopeStateStore.for_envelope(envelope)
    state = state_store.load()
    print(f"信封: {envelope.path}")
    print(f"SHA-256: {envelope.sha256}")
    print(
        "有效期: "
        f"{envelope.valid_from.isoformat()}"
        f" 至 {envelope.valid_until.isoformat()}"
    )
    print(f"品种: {sorted(str(s) for s in envelope.symbols)}")
    print(
        f"单笔上限 {envelope.order_jpy_max} JPY /"
        f" 当日 {envelope.day_jpy_max} JPY"
        f" {envelope.day_count_max} 笔"
    )
    print(
        f"信封总额 {envelope.envelope_jpy_total} JPY"
        f" / 首单 canary {envelope.canary_first_order_jpy_max} JPY"
    )
    print(
        f"持仓上限 {envelope.max_position_jpy} JPY /"
        f" 累计亏损 {envelope.max_cumulative_loss_jpy} JPY /"
        f" 当日亏损 {envelope.day_loss_jpy_max} JPY"
    )
    print(f"熔断动作: {envelope.on_trip.value}")
    if envelope.envelope_jpy_total == envelope.order_jpy_max:
        print("形态: 首封（总额等于单笔上限，只容一笔，T-12）")
    else:
        print("形态: 常规信封")
    used = usage.total_jpy()
    print(
        f"用量文件: {usage.path}"
        f"（{'已存在，已用 ' + format(used, 'f') + ' JPY' if used else '新身份'}）"
    )
    if state.tripped_at is not None:
        print(
            f"警告: 该身份已于 {state.tripped_at.isoformat()} 熔断锁定:"
            f" {state.trip_reason}"
        )
        return 1
    print(f"状态文件: {state_store.path}（未熔断）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
