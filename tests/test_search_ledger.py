"""试验台账全量登记与内容寻址测试（纯 CPU）。"""
from __future__ import annotations

from pathlib import Path

import pytest

from guvolu.search.ledger import (
    STAGE_F0_REJECTED,
    STAGE_F1_SCREENED,
    STAGE_F3_EXACT,
    LedgerRow,
    TrialLedgerWriter,
    ledger_header,
    read_ledger,
)


def _row(index: int, stage: str) -> LedgerRow:
    """构造一条测试行。"""
    return LedgerRow(
        evaluation_id=f"evaluation-{index:064x}",
        candidate_id=f"candidate-{index:064x}",
        family="trend",
        bundle_id="search-bundle-" + "a" * 64,
        stage=stage,
        device="cpu",
        precision="f32",
        metrics=None if stage == STAGE_F0_REJECTED else {"sharpe": 0.1, "turnover": 1.0},
        parity=None,
        screen_passed=None if stage == STAGE_F0_REJECTED else True,
        promotable=stage == STAGE_F3_EXACT,
        reason="lookback_not_in_panel" if stage == STAGE_F0_REJECTED else None,
    )


def test_ledger_registers_every_candidate_including_rejected(tmp_path: Path) -> None:
    """被拒候选也必须入账，完成后文件名即内容散列。"""
    writer = TrialLedgerWriter(tmp_path)
    writer.append_header(ledger_header(
        "search-bundle-" + "a" * 64, {"dtype": "f32"}, {"device": "cpu"},
    ))
    writer.append_rows([_row(0, STAGE_F0_REJECTED), _row(1, STAGE_F1_SCREENED)])
    writer.append_rows([_row(2, STAGE_F3_EXACT)])
    path, digest = writer.finalize()
    assert path.name == f"trial-ledger-{digest}.jsonl"
    header, rows = read_ledger(path)
    assert header["bundle_id"] == "search-bundle-" + "a" * 64
    assert [row["stage"] for row in rows] == [
        STAGE_F0_REJECTED, STAGE_F1_SCREENED, STAGE_F3_EXACT,
    ]
    assert rows[0]["reason"] == "lookback_not_in_panel"
    assert rows[0]["metrics"] is None
    assert rows[2]["promotable"] is True and rows[1]["promotable"] is False
    assert writer.rows_written == 3
    tampered = path.read_bytes() + b"{}\n"
    path.write_bytes(tampered)
    with pytest.raises(ValueError, match="散列"):
        read_ledger(path)


def test_ledger_rejects_unknown_stage_and_partial_collision(tmp_path: Path) -> None:
    """非法阶段与未完成台账残留必须拒绝。"""
    writer = TrialLedgerWriter(tmp_path)
    with pytest.raises(ValueError, match="阶段"):
        writer.append_rows([_row(0, "F2_unknown")])
    writer.append_header(ledger_header("b", {}, {}))
    with pytest.raises(FileExistsError):
        TrialLedgerWriter(tmp_path)
