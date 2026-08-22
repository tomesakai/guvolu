"""只读检查活动成交 head 的决策级经济成交资格。"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from guvolu.research.panel import freeze_trade_inputs
from guvolu.research.provenance import canonical_json


def main(argv: Sequence[str] | None = None) -> int:
    """打印机器可读摘要；存在不合格行时返回退出码 2。"""
    parser = argparse.ArgumentParser(description="检查活动成交经济语义")
    parser.add_argument("market_id")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    arguments = parser.parse_args(argv)
    inputs = freeze_trade_inputs(
        arguments.data_root.resolve(), arguments.market_id,
    )
    print(canonical_json({
        "market_id": arguments.market_id,
        "head_generation": inputs.head_generation,
        "trade_flow_input_method_version": (
            inputs.trade_flow_input_method_version
        ),
        "normalization_versions": list(inputs.normalization_versions),
        "input_file_count": len(inputs.paths),
        "source_trade_rows": inputs.source_trade_rows,
        "economic_trade_rows": inputs.economic_trade_rows,
        "unqualified_trade_rows": inputs.unqualified_trade_rows,
        "volume_qualified": inputs.volume_qualified,
    }))
    return 0 if inputs.volume_qualified else 2


if __name__ == "__main__":
    raise SystemExit(main())
