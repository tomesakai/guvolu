"""SearchFast 入口脚本与运行编排测试：导出、评估、数值对照与台账全量登记。"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from guvolu.search.bundle import load_search_bundle
from guvolu.search.ledger import (
    STAGE_F0_REJECTED,
    STAGE_F1_SCREENED,
    STAGE_F3_EXACT,
    read_ledger,
)
from guvolu.search.runner import (
    REASON_LOOKBACK,
    ScreenConfig,
    load_search_result,
    screen_from_config,
    static_gate,
)
from guvolu.search.kernels import parse_family_plans
from guvolu.search.torch_runtime import cuda_available, torch_module_or_none
from scripts import run_search_fast
from searchfast_support import build_fixture

FAKE_DIGEST = "d" * 64


@pytest.fixture(autouse=True)
def _no_research_import(monkeypatch: pytest.MonkeyPatch) -> None:
    """测试中以固定代码树摘要代替研究侧代码身份读取。"""
    monkeypatch.setattr(run_search_fast, "_code_tree_digest", lambda _paths: FAKE_DIGEST)


def test_export_writes_bundle_and_reference_panel(tmp_path: Path) -> None:
    """export 只在 CPU 运行，写出内容寻址搜索束与参考面板。"""
    summary = run_search_fast.main([
        "export", "--output", str(tmp_path),
        "--synthetic-bars", "64", "--synthetic-seed", "5", "--lookbacks", "4,8",
    ])
    directory = Path(str(summary["bundle_directory"]))
    assert directory.name == summary["bundle_id"]
    bundle = load_search_bundle(directory)
    assert bundle.identity.code_tree_digest == FAKE_DIGEST
    assert bundle.panel.bar_count == 64
    panel_path = Path(str(summary["reference_panel"]))
    assert panel_path.exists()
    payload = json.loads(panel_path.read_text(encoding="utf-8"))
    assert len(payload["bars"]) == 64


def test_static_gate_rejects_unknown_lookback_and_negative_sizing() -> None:
    """F0 静态闸门拒绝面板缺窗与负 sizing 参数。"""
    fixture = build_fixture(bars=8, seed=1, families=("trend",))
    family = parse_family_plans(fixture.plan)[0]
    row = list(family.parameter_rows[0])
    assert static_gate(family, row, (4, 8)) is None
    assert static_gate(family, row, (4,)) in (None, REASON_LOOKBACK)
    row[family.parameter_names.index("lookback")] = 99
    assert static_gate(family, row, (4, 8)) == REASON_LOOKBACK
    row[family.parameter_names.index("lookback")] = 4
    row[family.parameter_names.index("maximum_target")] = -1.0
    assert static_gate(family, row, (4, 8)) == "negative_sizing_parameter"
    assert screen_from_config({"minimum_sharpe": 0.5}) == ScreenConfig(minimum_sharpe=0.5)
    with pytest.raises(ValueError, match="粗筛阈值"):
        screen_from_config({"minimum_sharpe": "x"})


@pytest.mark.skipif(torch_module_or_none() is None, reason="未安装 torch")
@pytest.mark.parametrize(
    "device", ("cpu", "cuda") if cuda_available() else ("cpu",),
)
def test_selfcheck_runs_export_evaluate_parity(tmp_path: Path, device: str) -> None:
    """自检串联三步：台账全量含 F0，晋级候选经 CPU 精确复算通过。"""
    summary = run_search_fast.main([
        "--seed", "3",
        "selfcheck", "--output", str(tmp_path),
        "--synthetic-bars", "256", "--synthetic-seed", "9", "--lookbacks", "4,8",
        "--device", device, "--candidate-chunk", "7",
    ])
    result_directory = Path(str(summary["search_result_directory"]))
    manifest = load_search_result(result_directory)
    assert manifest["bundle_id"] == summary["bundle_id"]
    assert manifest["runtime"]["device"] == device
    assert manifest["options"]["seed"] == 3
    assert manifest["worker_code_identity"]["git_hash"]
    bundle = load_search_bundle(Path(str(summary["bundle_directory"])))
    candidate_total = sum(
        len(family.parameter_rows) for family in parse_family_plans(bundle.search_plan)
    )
    ledger_record = manifest["trial_ledger"]
    header, rows = read_ledger(result_directory / str(ledger_record["path"]))
    assert header["bundle_id"] == summary["bundle_id"]
    assert len(rows) == candidate_total == manifest["candidate_count"]
    assert all(row["stage"] == STAGE_F1_SCREENED for row in rows)
    assert len({row["evaluation_id"] for row in rows}) == candidate_total
    parity_directory = Path(str(summary["parity_directory"]))
    parity_summary = json.loads(
        (parity_directory / "parity-summary.json").read_text(encoding="utf-8"),
    )
    assert parity_summary["checked"] > 0
    assert parity_summary["failed"] == 0
    assert parity_summary["max_abs_diff"]["target"] <= 1e-5
    assert parity_summary["max_abs_diff"]["sharpe"] <= 1e-3
    assert parity_summary["max_abs_diff"]["turnover"] <= 1e-6
    _parity_header, parity_rows = read_ledger(
        parity_directory / str(parity_summary["parity_ledger"]["path"]),
    )
    assert parity_rows and all(row["stage"] == STAGE_F3_EXACT for row in parity_rows)
    assert all(row["promotable"] for row in parity_rows)
    assert all(row["parity"]["passed"] for row in parity_rows)
    screened_passed = sum(bool(row["screen_passed"]) for row in rows)
    assert len(parity_rows) == screened_passed


@pytest.mark.skipif(torch_module_or_none() is None, reason="未安装 torch")
def test_evaluate_registers_f0_rejections_in_ledger(tmp_path: Path) -> None:
    """面板缺少候选回看窗时候选以 F0 入账，且不进入粗筛。"""
    export = run_search_fast.main([
        "export", "--output", str(tmp_path / "bundle"),
        "--synthetic-bars", "48", "--synthetic-seed", "2", "--lookbacks", "4",
        "--strategy-config", str(_config_with_extra_lookback(tmp_path)),
    ])
    result = run_search_fast.main([
        "evaluate", "--bundle", str(export["bundle_directory"]),
        "--output", str(tmp_path / "result"), "--device", "cpu",
    ])
    manifest = load_search_result(Path(str(result["search_result_directory"])))
    _header, rows = read_ledger(
        Path(str(result["search_result_directory"])) / str(manifest["trial_ledger"]["path"]),
    )
    stages = {row["stage"] for row in rows}
    assert stages == {STAGE_F0_REJECTED, STAGE_F1_SCREENED}
    rejected = [row for row in rows if row["stage"] == STAGE_F0_REJECTED]
    assert all(row["reason"] == REASON_LOOKBACK for row in rejected)
    assert all(row["metrics"] is None for row in rejected)
    assert manifest["rejected_count"] == len(rejected) > 0
    assert manifest["candidate_count"] == len(rows)


def _config_with_extra_lookback(directory: Path) -> Path:
    """写出含面板缺失回看窗的策略配置。"""
    config = {
        "features": {"lookbacks": [4, 8]},
        "strategies": {
            "trend": {
                "lookbacks": [4, 8],
                "entry_scores": [0.5],
                "exit_score": 0.0,
                "annual_volatility_target": 0.4,
                "maximum_target": 1.0,
            },
        },
    }
    path = directory / "strategy.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path
