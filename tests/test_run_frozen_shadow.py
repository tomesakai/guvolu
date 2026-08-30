"""冻结前向每小时 shadow 链的 paper 接线合同。"""
from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import run_frozen_shadow as shadow  # noqa: E402

PLAN_ID = "frozen-forward-plan-" + "c" * 64
PREDICTION_ID = "frozen-forward-prediction-shadow-test"
MARKET_ID = "mkt__gmo__btc__r0"
ORDER_ENDPOINT = "POST /v1/order"


def _option(command: Sequence[str], name: str) -> str | None:
    """取命令行中紧随参数名的值。"""
    parts = list(command)
    if name not in parts:
        return None
    return parts[parts.index(name) + 1]


def _paper_filled_report() -> dict[str, object]:
    return {
        "mode": "paper",
        "prediction_id": PREDICTION_ID,
        "status": "PAPER_FILLED",
        "position_after": "0.00003",
        "intent": {"intent_id": "it-paper", "state": "PAPER_FILLED", "reason": None},
        "fill": {"fill_size": "0.00003", "model_fill_price": "15000000"},
        "cost": {"total_cost_bps": "6.5"},
        "endpoints": {
            "read_touched": ["GET /v1/orderbooks"],
            "write_planned": [],
            "write_touched": [],
        },
    }


class FakeChain:
    """以假子进程落盘预测、目标与报告，并记录每次调用。"""

    def __init__(self, tmp_path: Path) -> None:
        self.repository = tmp_path / "repository"
        self.runtime = tmp_path / "runtime"
        self.execution = tmp_path / "execution"
        for root in (self.repository / "data", self.runtime, self.execution):
            root.mkdir(parents=True)
        self.calls: list[list[str]] = []
        self.paper_report: dict[str, object] | None = _paper_filled_report()
        self.paper_returncode = 0
        self.prediction_decision_time: datetime | None = None

    def refresh(self, source: Path, runtime: Path, market_id: str) -> dict[str, object]:
        return {"status": "refreshed", "market_id": market_id}

    def run(
        self, command: Sequence[str], *, cwd: Path, env: object = None,
    ) -> subprocess.CompletedProcess[str]:
        parts = [str(part) for part in command]
        self.calls.append(parts)
        script = Path(parts[1]).name
        handlers = {
            "manage_frozen_forward.py": self._predict,
            "adapt_frozen_target.py": self._adapt,
            "run_dry_run_executor.py": self._dry_run,
            "run_paper_executor.py": self._paper,
        }
        return handlers[script](parts)

    def scripts(self, name: str) -> list[list[str]]:
        return [call for call in self.calls if Path(call[1]).name == name]

    def _predict(self, parts: list[str]) -> subprocess.CompletedProcess[str]:
        path = self.runtime / "predictions" / f"{PREDICTION_ID}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
        payload = {
            "prediction_id": PREDICTION_ID,
            "prediction_path": str(path),
            "decision_time": (
                self.prediction_decision_time or datetime.now(UTC)
            ).isoformat(),
            "aggregate_target": 0.4,
        }
        return subprocess.CompletedProcess(parts, 0, json.dumps(payload) + "\n", "")

    def _adapt(self, parts: list[str]) -> subprocess.CompletedProcess[str]:
        mode = _option(parts, "--mode") or "dry-run"
        output = Path(_option(parts, "--output-directory") or "")
        output.mkdir(parents=True, exist_ok=True)
        target = output / f"target-{mode}.json"
        target.write_text(json.dumps({"mode": mode, "run_id": PREDICTION_ID}), encoding="utf-8")
        payload = {
            "path": str(target),
            "sha256": mode,
            "status": f"ready_for_{mode.replace('-', '_')}",
        }
        return subprocess.CompletedProcess(parts, 0, json.dumps(payload) + "\n", "")

    def _dry_run(self, parts: list[str]) -> subprocess.CompletedProcess[str]:
        report = Path(_option(parts, "--dry-run-report") or "")
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps({
            "mode": "dry-run",
            "artifact": {"run_id": PREDICTION_ID},
            "intent": {"state": "DRY_RUN_REJECTED"},
            "endpoints": {"write_touched": []},
        }), encoding="utf-8")
        return subprocess.CompletedProcess(parts, 0, "", "")

    def _paper(self, parts: list[str]) -> subprocess.CompletedProcess[str]:
        report = Path(_option(parts, "--report") or "")
        if self.paper_report is not None:
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text(json.dumps(self.paper_report), encoding="utf-8")
        return subprocess.CompletedProcess(
            parts, self.paper_returncode, "", "paper 进程异常",
        )


@pytest.fixture
def chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeChain:
    fake = FakeChain(tmp_path)
    monkeypatch.setattr(shadow, "refresh_runtime", fake.refresh)
    monkeypatch.setattr(shadow, "_run", fake.run)
    return fake


def _run_chain(chain: FakeChain, **overrides: object) -> dict[str, object]:
    return shadow.run_shadow(
        chain.repository, chain.runtime, chain.execution, PLAN_ID, MARKET_ID,
        **overrides,  # type: ignore[arg-type]
    )


def _task_records(chain: FakeChain) -> list[dict[str, object]]:
    path = chain.execution / "data/execution/shadow/frozen-forward/task.jsonl"
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_chain_runs_paper_after_dry_run_and_records_fields(chain: FakeChain) -> None:
    summary = _run_chain(chain)
    paper = summary["paper"]
    assert isinstance(paper, dict)

    assert summary["status"] == "completed"
    assert summary["intent_state"] == "DRY_RUN_REJECTED"
    assert paper["status"] == "completed"
    assert paper["outcome"] == "PAPER_FILLED"
    assert paper["intent_state"] == "PAPER_FILLED"
    assert paper["returncode"] == 0
    assert paper["fill"] == {"fill_size": "0.00003", "model_fill_price": "15000000"}
    assert paper["cost"] == {"total_cost_bps": "6.5"}
    assert paper["position_after"] == "0.00003"
    assert paper["write_touched"] == []
    assert paper["read_touched"] == ["GET /v1/orderbooks"]

    adapters = chain.scripts("adapt_frozen_target.py")
    assert [_option(call, "--mode") for call in adapters] == ["dry-run", "paper"]
    config = str(chain.execution / "config/paper_executor.json")
    assert all(_option(call, "--config") == config for call in adapters)
    assert _option(adapters[0], "--risk-budget-jpy") == "500"
    assert _option(adapters[1], "--risk-budget-jpy") is None
    assert paper["target_path"] != summary["target_path"]
    assert Path(str(paper["target_path"])).name == "target-paper.json"

    paper_calls = chain.scripts("run_paper_executor.py")
    assert len(paper_calls) == 1
    assert _option(paper_calls[0], "--ledger-root") == str(chain.execution / "data")
    assert _option(paper_calls[0], "--config") == config
    assert _option(paper_calls[0], "--target") == paper["target_path"]
    assert Path(str(paper["report_path"])) == (
        chain.execution / "data/execution/paper/reports" / f"{PREDICTION_ID}.json"
    )
    order = [Path(call[1]).name for call in chain.calls]
    assert order.index("run_dry_run_executor.py") < order.index("run_paper_executor.py")

    records = _task_records(chain)
    assert len(records) == 1
    assert records[0]["status"] == "completed"
    assert records[0]["paper"] == paper


def test_paper_failure_keeps_prediction_and_dry_run_result(
    chain: FakeChain, capsys: pytest.CaptureFixture[str],
) -> None:
    chain.paper_report = None
    chain.paper_returncode = 3

    code = shadow.main([
        "--repository", str(chain.repository),
        "--runtime-root", str(chain.runtime),
        "--execution-repository", str(chain.execution),
        "--plan-id", PLAN_ID,
    ])
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    paper = summary["paper"]

    assert code == 1
    assert summary["status"] == "completed"
    assert summary["prediction_id"] == PREDICTION_ID
    assert summary["intent_state"] == "DRY_RUN_REJECTED"
    assert summary["write_touched"] == []
    assert paper["status"] == "failed"
    assert "paper 执行失败(3)" in paper["error"]
    assert "paper 进程异常" in paper["error"]
    assert "outcome" not in paper
    record = _task_records(chain)[-1]
    recorded_paper = record["paper"]
    assert isinstance(recorded_paper, dict)
    assert record["status"] == "completed"
    assert recorded_paper["status"] == "failed"


def test_stale_prediction_fails_before_target_adaptation(chain: FakeChain) -> None:
    """超过保守年龄上限的冻结预测不得进入任何执行适配。"""
    chain.prediction_decision_time = datetime.now(UTC) - timedelta(minutes=51)

    with pytest.raises(ValueError, match="冻结预测过期"):
        _run_chain(chain)

    assert chain.scripts("adapt_frozen_target.py") == []
    assert _task_records(chain)[-1]["status"] == "failed"


@pytest.mark.parametrize(
    ("patch", "message"),
    [
        ({"endpoints": {"read_touched": [], "write_planned": [], "write_touched": [ORDER_ENDPOINT]}}, "触及了写端点"),
        ({"mode": "dry-run"}, "不是 paper 模式"),
        ({"prediction_id": "frozen-forward-prediction-other"}, "预测身份不符"),
    ],
)
def test_invalid_paper_report_is_recorded_as_failure(
    chain: FakeChain, patch: dict[str, object], message: str,
) -> None:
    report = _paper_filled_report()
    report.update(patch)
    chain.paper_report = report

    summary = _run_chain(chain)
    paper = summary["paper"]
    assert isinstance(paper, dict)

    assert summary["status"] == "completed"
    assert paper["status"] == "failed"
    assert message in str(paper["error"])
    assert "outcome" not in paper


def test_duplicate_paper_report_without_mode_is_accepted(chain: FakeChain) -> None:
    chain.paper_report = {
        "prediction_id": PREDICTION_ID,
        "status": "duplicate_prediction",
        "endpoints": {"read_touched": [], "write_planned": [], "write_touched": []},
    }

    paper = _run_chain(chain)["paper"]
    assert isinstance(paper, dict)

    assert paper["status"] == "completed"
    assert paper["outcome"] == "duplicate_prediction"
    assert paper["intent_state"] is None
    assert paper["fill"] is None


def test_nonzero_paper_report_is_failed_even_when_report_is_valid(
    chain: FakeChain,
) -> None:
    """执行器非零码不能被已落盘的合法待对账报告掩盖。"""
    chain.paper_report = {
        "prediction_id": PREDICTION_ID,
        "status": "needs_reconciliation",
        "endpoints": {
            "read_touched": [], "write_planned": [], "write_touched": [],
        },
    }
    chain.paper_returncode = 1

    paper = _run_chain(chain)["paper"]
    assert isinstance(paper, dict)
    assert paper["status"] == "failed"
    assert paper["outcome"] == "needs_reconciliation"
    assert paper["returncode"] == 1
    assert "非零码" in str(paper["error"])


def test_reused_reconciliation_report_remains_failed(chain: FakeChain) -> None:
    """待对账报告复用时仍须非零，不得伪装为健康周期。"""
    report = chain.execution / (
        "data/execution/paper/reports/" + PREDICTION_ID + ".json"
    )
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps({
        "prediction_id": PREDICTION_ID,
        "status": "needs_reconciliation",
        "endpoints": {
            "read_touched": [], "write_planned": [], "write_touched": [],
        },
    }), encoding="utf-8")

    paper = _run_chain(chain)["paper"]
    assert isinstance(paper, dict)
    assert paper["status"] == "failed"
    assert paper["outcome"] == "needs_reconciliation"
    assert paper["returncode"] is None
    assert "人工处置" in str(paper["error"])
    assert chain.scripts("run_paper_executor.py") == []


def test_no_paper_skips_paper_step(chain: FakeChain) -> None:
    summary = _run_chain(chain, paper_enabled=False)

    assert summary["status"] == "completed"
    assert summary["paper"] == {"status": "skipped", "reason": "--no-paper"}
    assert chain.scripts("run_paper_executor.py") == []
    assert [_option(call, "--mode") for call in chain.scripts("adapt_frozen_target.py")] == ["dry-run"]
    assert _task_records(chain)[-1]["paper"] == {"status": "skipped", "reason": "--no-paper"}


def test_rerun_reuses_dry_run_and_paper_reports(chain: FakeChain) -> None:
    first = _run_chain(chain)
    second = _run_chain(chain)
    first_paper = first["paper"]
    paper = second["paper"]
    assert isinstance(first_paper, dict)
    assert isinstance(paper, dict)

    assert first["status"] == "completed"
    assert second["status"] == "reused"
    assert paper["status"] == "reused"
    assert paper["returncode"] is None
    assert paper["outcome"] == "PAPER_FILLED"
    assert paper["report_path"] == first_paper["report_path"]
    assert len(chain.scripts("run_dry_run_executor.py")) == 1
    assert len(chain.scripts("run_paper_executor.py")) == 1
    assert [record["status"] for record in _task_records(chain)] == ["completed", "reused"]
