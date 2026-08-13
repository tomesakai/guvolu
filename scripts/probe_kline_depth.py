"""补充探测：分钟线历史深度与逐笔成交翻页深度。

回答两个回补问题（TBD-04）：
一、短周期 KLine 能回取多早的交易日。
二、REST 逐笔成交最多能翻多少页。
复用主拉取脚本的落盘格式，追加写入同日目录。
"""
from __future__ import annotations

import dataclasses
import sys
from datetime import UTC, datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS_DIR))

from fetch_full_snapshot import RAW_ROOT, Fetcher, RawWriter, rows_of

from guvolu.domain.config import load_config
from guvolu.domain.ids import new_run_id

REPO_ROOT = SCRIPTS_DIR.parent

# 分钟线深度探测交易日
PROBE_DATES = (
    "20180103",
    "20190103",
    "20200103",
    "20210103",
    "20210415",
    "20220103",
    "20230103",
    "20240103",
    "20250103",
    "20260105",
)
# 逐笔成交翻页探测页码
PROBE_PAGES = (10, 20, 50, 90, 100)


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    run_id = new_run_id()
    config = load_config(REPO_ROOT / ".env")
    # 本进程只读，剥离 TRADE 密钥
    config = dataclasses.replace(
        config, trade_api_key=None, trade_api_secret=None
    )
    day_dir = RAW_ROOT / datetime.now(UTC).strftime("%Y-%m-%d")
    writer = RawWriter(day_dir, run_id)
    fetcher = Fetcher(config, writer, run_id)
    print(f"run_id {run_id} 追加目录 {day_dir}")

    print("探测 BTC 分钟线历史深度")
    for interval in ("1min", "1hour"):
        for date in PROBE_DATES:
            data = fetcher.get(
                "/v1/klines",
                {"symbol": "BTC", "interval": interval, "date": date},
            )
            rows = rows_of(data)
            print(f"  {interval} {date}: {len(rows)} 根")

    print("探测 BTC 逐笔成交翻页深度")
    for page in PROBE_PAGES:
        data = fetcher.get(
            "/v1/trades", {"symbol": "BTC", "count": 100, "page": page}
        )
        rows = rows_of(data)
        stamp = str(rows[-1].get("timestamp")) if rows else "无"
        print(f"  page={page}: {len(rows)} 笔 末笔时刻 {stamp}")

    print(f"完成 请求 {fetcher.request_total} 错误 {fetcher.error_total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
