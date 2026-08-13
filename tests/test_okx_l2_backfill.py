from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from guvolu.data import store
from guvolu.data.okx_l2_backfill import (
    GIB,
    plan_okx_l2_backfill,
    plan_summary,
    required_free_bytes,
)


def test_okx_l2_plan_derives_pending_days_without_progress_file(
    tmp_path: Path,
) -> None:
    conn = store.connect(tmp_path)
    try:
        plan = plan_okx_l2_backfill(
            tmp_path,
            conn,
            venue_symbol="BTC-USDT",
            from_day=date(2026, 8, 7),
            to_day=date(2026, 8, 8),
        )

        summary = plan_summary(plan)

        assert [task.status for task in plan.tasks] == ["pending", "pending"]
        assert summary["pending_days"] == 2
        assert summary["active_days"] == 0
        assert summary["estimated_additional_bytes"] > 0
    finally:
        conn.close()


def test_okx_l2_disk_gate_keeps_reserve_and_working_space() -> None:
    required = required_free_bytes(111_062_744, reserve_gib=20)

    assert required >= 30 * GIB


def test_okx_l2_plan_rejects_unverified_5000_depth(
    tmp_path: Path,
) -> None:
    conn = store.connect(tmp_path)
    try:
        with pytest.raises(ValueError, match="5000"):
            plan_okx_l2_backfill(
                tmp_path,
                conn,
                venue_symbol="BTC-USDT",
                from_day=date(2026, 8, 7),
                to_day=date(2026, 8, 7),
                depth_levels=5000,
            )
    finally:
        conn.close()
