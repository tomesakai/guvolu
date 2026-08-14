"""市场数据守护配置的静态部署合同。"""
from __future__ import annotations

from pathlib import Path


def test_forward_minimal_profile_preserves_raw_and_required_trade_path() -> None:
    """最小配置不得缩减原始采集，且只保留逐笔物化。"""
    script = Path("scripts/start_marketdata_pipeline.ps1").read_text(encoding="utf-8")

    for name in (
        "l2-gmo-btc",
        "l2-bitbank-btc-jpy",
        "l2-bitflyer-btc-jpy",
        "trade-gmo-btc",
        "trade-bitbank-btc-jpy",
        "trade-bitflyer-btc-jpy",
    ):
        assert f"Name = '{name}'" in script
    assert "[ValidateSet('Full', 'ForwardMinimal')]" in script
    assert "Where-Object { $_.Name -eq 'trade-realtime-materializer' }" in script
    assert "Where-Object { $_.Name -ne 'trade-realtime-materializer' }" in script
    assert "Stop-Process -Id $Process.ProcessId -Force" in script
    assert "$RunnerRoot = Join-Path $RepoRoot 'scripts'" in script
    assert "Set-Location -LiteralPath $RepoRoot" in script
    assert "Join-Path $PSScriptRoot $Collector.Runner" not in script
    assert "Join-Path $PSScriptRoot $Materializer.Runner" not in script


def test_task_registration_pins_profile_and_resolved_repository() -> None:
    """任务动作必须显式传递受限配置和仓库绝对路径。"""
    script = Path("scripts/register_marketdata_tasks.ps1").read_text(
        encoding="utf-8"
    )

    assert "[ValidateSet('Full', 'ForwardMinimal')]" in script
    assert "(Resolve-Path -LiteralPath $Repository).Path" in script
    assert '"-Profile $Profile -Repository `"$RepoRoot`""' in script
