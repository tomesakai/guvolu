"""冻结前向 shadow 链的不可变来源、执行报告与成功回执合同。"""
from __future__ import annotations

import ctypes
import hashlib
import json
import os
import py_compile
import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, BinaryIO

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import run_frozen_shadow as shadow  # noqa: E402

PLAN_ID = "frozen-forward-plan-" + "c" * 64
PREDICTION_ID = "frozen-forward-prediction-" + "d" * 64
MARKET_ID = "mkt__gmo__btc__r0"
ORDER_ENDPOINT = "POST /v1/order"


def _option(command: Sequence[str], name: str) -> str | None:
    """取命令行中紧随参数名的值。"""
    parts = list(command)
    if name not in parts:
        return None
    return parts[parts.index(name) + 1]


def _script_name(command: Sequence[str]) -> str:
    for item in command:
        path = Path(str(item))
        if path.suffix == ".py":
            return path.name
    raise AssertionError(f"命令缺少 Python 入口: {command!r}")


def _json_object(body: str | bytes) -> dict[str, object]:
    value: object = json.loads(body)
    assert isinstance(value, dict)
    return {str(key): item for key, item in value.items()}


def _canonical(value: Mapping[str, object]) -> bytes:
    return (json.dumps(
        dict(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    ) + "\n").encode("utf-8")


def _make_directory_alias(target: Path, link: Path) -> None:
    if os.name == "nt":
        completed = subprocess.run(
            ["cmd.exe", "/d", "/c", "mklink", "/J", str(link), str(target)],
            check=False,
            capture_output=True,
        )
        if completed.returncode != 0:
            pytest.skip("当前 Windows 环境无法创建目录 junction")
        return
    try:
        os.symlink(target, link, target_is_directory=True)
    except OSError:
        pytest.skip("当前平台无法创建目录 symlink")


def _remove_directory_alias(link: Path) -> None:
    if link.is_symlink():
        link.unlink()
    else:
        link.rmdir()


def _windows_process_active(process_id: int) -> bool:
    loader = getattr(ctypes, "WinDLL")
    kernel = loader("kernel32", use_last_error=True)
    kernel.OpenProcess.restype = ctypes.c_void_p
    handle = kernel.OpenProcess(0x1000, False, process_id)
    if handle is None:
        return False
    try:
        exit_code = ctypes.c_uint32()
        if not kernel.GetExitCodeProcess(
            ctypes.c_void_p(handle), ctypes.byref(exit_code),
        ):
            return False
        return exit_code.value == 259
    finally:
        kernel.CloseHandle(ctypes.c_void_p(handle))


def _paper_filled_report() -> dict[str, object]:
    return {
        "schema_version": 1,
        "record": "paper_decision",
        "at": "2026-08-26T00:00:00+00:00",
        "mode": "paper",
        "prediction_id": PREDICTION_ID,
        "decision_time": "2026-08-26T00:00:00+00:00",
        "valid_until": "2026-08-26T01:00:00+00:00",
        "correlation_id": "co-paper",
        "market_id": MARKET_ID,
        "symbol": "BTC",
        "target_path": "filled-by-fake",
        "target_sha256": "0" * 64,
        "exposure_target": 0.4,
        "risk_budget_jpy": "500",
        "target_notional_jpy": "200.0000000000000000000000000",
        "reference_price": "15000000",
        "position_before": "0",
        "status": "PAPER_FILLED",
        "position_after": "0.00001",
        "book_error": None,
        "delta": {
            "desired_size": "0.00001333333333333333333333333333",
            "position_size": "0",
            "delta_size": "0.00001333333333333333333333333333",
            "proposal": {
                "symbol": "BTC", "side": "BUY", "size": "0.00001",
                "price": "15000000", "notional_jpy": "150.00000",
            },
            "skip_reason": None,
        },
        "intent": {
            "intent_id": "it-paper", "side": "BUY", "size": "0.00001",
            "price": "15000000", "state": "PAPER_FILLED", "reason": None,
        },
        "fill": {
            "side": "BUY", "fill_size": "0.00001",
            "expected_price": "15000000", "model_fill_price": "15000000",
            "notional_jpy": "150.00000", "fee_jpy": "0.075",
            "levels_consumed": 1, "fill_basis": "PUBLIC_ORDERBOOK",
            "fee_source": "fixture", "book_observed_at": "2026-08-26T00:00:00+00:00",
        },
        "cost": {
            "fee_bps": "5", "half_spread_bps": "1", "impact_bps": "0",
            "slippage_vs_reference_bps": "0", "total_cost_bps": "5",
        },
        "fee": {"bps": "5", "source": "fixture", "detail": None},
        "overlay": {},
        "service_status": "OPEN",
        "endpoints": {
            "read_touched": ["GET /v1/orderbooks"],
            "write_planned": [],
            "write_touched": [],
        },
        "generated_at": "2026-08-26T00:00:01+00:00",
        "ledger_paths": {},
        "startup": {
            "recovered_sends": {
                "intent_ids": [], "state": "PAPER_REJECTED",
                "reason": "paper 启动恢复",
            },
            "limit_usage": {
                "trading_day": "2026-08-26", "total_jpy": "0",
                "order_count": 0, "replayed_intents": [],
            },
        },
    }


def _duplicate_paper_report() -> dict[str, object]:
    return {
        "generated_at": "2026-08-26T00:00:01+00:00",
        "target_path": "filled-by-fake",
        "target_sha256": "0" * 64,
        "ledger_paths": {},
        "prediction_id": PREDICTION_ID,
        "status": "duplicate_prediction",
        "endpoints": {
            "read_touched": [], "write_planned": [], "write_touched": [],
        },
        "startup": {
            "recovered_sends": {
                "intent_ids": [], "state": "PAPER_REJECTED",
                "reason": "paper 启动恢复",
            },
            "limit_usage": {
                "trading_day": "2026-08-26", "total_jpy": "0",
                "order_count": 0, "replayed_intents": [],
            },
        },
    }


class FakeChain:
    """以内容寻址假制品执行完整链，并记录全部子进程边界。"""

    def __init__(self, tmp_path: Path) -> None:
        self.repository = tmp_path / "repository"
        self.code = tmp_path / "code"
        self.runtime = tmp_path / "runtime"
        self.execution = tmp_path / "execution"
        for root in (
            self.repository / "data", self.code, self.runtime, self.execution,
        ):
            root.mkdir(parents=True)
        (self.runtime / "src").mkdir()
        (self.execution / "src").mkdir()
        for root, name in (
            (self.code, "run_frozen_shadow.py"),
            (self.code, "refresh_frozen_runtime.py"),
            (self.runtime, "manage_frozen_forward.py"),
            (self.execution, "adapt_frozen_target.py"),
            (self.execution, "run_dry_run_executor.py"),
            (self.execution, "run_paper_executor.py"),
        ):
            tracked = root / "scripts" / name
            tracked.parent.mkdir(parents=True, exist_ok=True)
            tracked.write_text(f"# {name}\n", encoding="utf-8")
        self.runner_python = self.code / ".venv/Scripts/python.exe"
        self.runner_python.parent.mkdir(parents=True)
        self.runner_python.write_bytes(b"fixture-runner-python\n")
        self.expected_python_sha256 = shadow._sha256(self.runner_python)
        self.git_executable = tmp_path / "tools/git.exe"
        self.git_executable.parent.mkdir()
        self.git_executable.write_bytes(b"fixture-git\n")
        self.expected_git_sha256 = shadow._sha256(self.git_executable)
        config = self.execution / shadow.PAPER_CONFIG
        config.parent.mkdir(parents=True)
        config.write_text(json.dumps({
            "schema_version": 1,
            "market_id": MARKET_ID,
            "symbol": "BTC",
            "bar_interval": "1hour",
            "risk_budget_jpy": "500",
            "no_trade_band": "0.01",
            "taker_fee_fallback_bps": "5",
            "taker_fee_cache_seconds": 86400,
            "overlay": {
                "limit": "0.3",
                "maximum_spread_bps": "10",
                "minimum_top5_depth_base": "0.5",
                "maximum_anchor_age_seconds": 300,
            },
            "ledger_directory": "execution/paper",
        }) + "\n", encoding="utf-8")
        python = self.execution / ".venv/Scripts/python.exe"
        python.parent.mkdir(parents=True)
        python.write_bytes(b"fixture-python-v1\n")
        (self.execution / ".venv/pyvenv.cfg").write_text(
            "fixture = v1\n", encoding="utf-8",
        )
        (self.execution / ".venv/Lib/site-packages").mkdir(parents=True)
        self.expected_environment_tree_sha256 = shadow._venv_inventory(
            self.execution / ".venv",
        )[3]
        self.calls: list[list[str]] = []
        self.call_environments: list[Mapping[str, str] | None] = []
        self.paper_report: dict[str, object] | None = _duplicate_paper_report()
        self.paper_returncode = 0
        self.paper_writes_report = True
        self.dry_returncode = 0
        self.dry_writes_report = True
        self.dry_report_patch: dict[str, object] = {}
        self.dry_artifact_patch: dict[str, object] = {}
        self.dry_proposal_patch: dict[str, object] = {}
        self.dry_delay_seconds = 0.0
        self.prediction_decision_time = datetime.now(UTC)
        self.prediction_sha_override: str | None = None
        self.prediction_id_override: str | None = None
        self.prediction_path_override: Path | None = None
        self.rewrite_prediction = True
        self.adapter_sha_override: str | None = None
        self.adapter_bad_filename = False
        self.adapter_path_override: Path | None = None
        self.target_patch: dict[str, object] = {}
        self.target_lineage_patch: dict[str, object] = {}
        self.mutate_origin_on_adapter = False
        self.origin_mutated = False
        self.execution_head = "e" * 40
        self.runtime_head = "b" * 40
        self.code_head = "a" * 40
        self.execution_dirty = False
        self.runtime_dirty = False
        self.code_dirty = False

    @property
    def prediction_path(self) -> Path:
        return self.runtime / "predictions" / f"{PREDICTION_ID}.json"

    def official_prediction(self) -> dict[str, object]:
        return {
            "schema_version": 2,
            "scope": "FROZEN_FORWARD",
            "prediction_id": PREDICTION_ID,
            "plan_id": PLAN_ID,
            "decision_time": self.prediction_decision_time.isoformat(),
            "aggregate_target": 0.4,
            "exposure_target": 0.4,
            "unit": "risk_weighted_directional_target",
            "input_head_generation": "fixture-head",
            "families": [],
            "reserve": 0.6,
            "quality": {
                "clock": True, "coverage": True, "eligible": True,
                "freshness": True, "integrity": True, "lineage": True,
                "pit": True, "reasons": [],
            },
        }

    def official_bytes(self) -> bytes:
        return _canonical(self.official_prediction())

    def refresh(self, source: Path, runtime: Path, market_id: str) -> dict[str, object]:
        return {"status": "refreshed", "market_id": market_id}

    def run(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        parts = [str(part) for part in command]
        self.calls.append(parts)
        self.call_environments.append(env)
        script = _script_name(parts)
        if script == "manage_frozen_forward.py":
            return self._predict(parts)
        if script == "refresh_frozen_runtime.py":
            return subprocess.CompletedProcess(
                parts, 0, json.dumps(self.refresh(
                    self.repository / "data", self.runtime, MARKET_ID,
                )) + "\n", "",
            )
        if script == "adapt_frozen_target.py":
            return self._adapt(parts)
        if script == "run_dry_run_executor.py":
            return self._dry_run(parts)
        if script == "run_paper_executor.py":
            return self._paper(parts)
        raise AssertionError(f"未知假子进程: {script}")

    def scripts(self, name: str) -> list[list[str]]:
        return [call for call in self.calls if _script_name(call) == name]

    def environments(self, name: str) -> list[Mapping[str, str] | None]:
        return [
            env
            for call, env in zip(self.calls, self.call_environments, strict=True)
            if _script_name(call) == name
        ]

    def _predict(self, parts: list[str]) -> subprocess.CompletedProcess[str]:
        body = self.official_bytes()
        if self.rewrite_prediction:
            self.prediction_path.parent.mkdir(parents=True, exist_ok=True)
            self.prediction_path.write_bytes(body)
        digest = hashlib.sha256(body).hexdigest()
        payload = {
            "prediction_id": self.prediction_id_override or PREDICTION_ID,
            "prediction_path": str(
                self.prediction_path_override or self.prediction_path
            ),
            "prediction_sha256": self.prediction_sha_override or digest,
            "decision_time": self.prediction_decision_time.isoformat(),
            "aggregate_target": 0.4,
        }
        return subprocess.CompletedProcess(parts, 0, json.dumps(payload) + "\n", "")

    def _adapt(self, parts: list[str]) -> subprocess.CompletedProcess[str]:
        mode = _option(parts, "--mode") or "dry-run"
        source = Path(_option(parts, "--prediction") or "")
        source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
        if self.mutate_origin_on_adapter and not self.origin_mutated:
            self.prediction_path.write_text('{"changed":"after-capture"}\n', encoding="utf-8")
            self.origin_mutated = True
        official = _json_object(source.read_bytes())
        decision = datetime.fromisoformat(str(official["decision_time"]))
        correlation = shadow._derive_correlation_id(PREDICTION_ID)
        budget = _option(parts, "--risk-budget-jpy") or "500"
        lineage: dict[str, object] = {
            "input_head_generation": "fixture-head",
            "plan_id": PLAN_ID,
            "prediction_id": PREDICTION_ID,
            "source_prediction_path": str(source),
            "source_prediction_sha256": source_sha256,
        }
        lineage.update(self.target_lineage_patch)
        target_payload: dict[str, object] = {
            "artifact_kind": "operational_target_snapshot",
            "bar_interval": "1hour",
            "correlation_id": correlation,
            "correlation_id_source": "adapter",
            "decision_time": official["decision_time"],
            "exposure_target": 0.4,
            "lineage": lineage,
            "market_id": MARKET_ID,
            "method_version": "frozen-forward-operational-target-v2",
            "mode": mode,
            "operational_target_contract": {
                "aggregate_target": 0.4,
                "families": [],
                "reserve": 0.6,
                "unit": "risk_weighted_directional_target",
            },
            "quality": official["quality"],
            "risk_budget_jpy": budget,
            "run_id": PREDICTION_ID,
            "schema_version": 2,
            "symbol": "BTC",
            "target_semantics": {
                "domain": "long_only_spot",
                "range": [0, 1],
                "reference": "fraction_of_risk_budget",
                "short_allowed": False,
            },
            "valid_from": official["decision_time"],
            "valid_until": (decision + timedelta(hours=1)).isoformat(),
            "valid_until_source": "derived",
        }
        target_payload.update(self.target_patch)
        target_body = _canonical(target_payload)
        actual_sha256 = hashlib.sha256(target_body).hexdigest()
        declared_sha256 = self.adapter_sha_override or actual_sha256
        output = Path(_option(parts, "--output-directory") or "")
        output.mkdir(parents=True, exist_ok=True)
        name = (
            "target-not-content-addressed.json"
            if self.adapter_bad_filename
            else f"target-{declared_sha256}.json"
        )
        target = output / name
        if not target.exists():
            target.write_bytes(target_body)
        payload = {
            "path": str(self.adapter_path_override or target),
            "sha256": declared_sha256,
            "status": f"ready_for_{mode.replace('-', '_')}",
        }
        return subprocess.CompletedProcess(parts, 0, json.dumps(payload) + "\n", "")

    def _dry_run(self, parts: list[str]) -> subprocess.CompletedProcess[str]:
        if self.dry_delay_seconds:
            time.sleep(self.dry_delay_seconds)
        report = Path(_option(parts, "--dry-run-report") or "")
        target = Path(_option(parts, "--target") or "")
        target_payload = _json_object(target.read_bytes())
        if self.dry_writes_report:
            report.parent.mkdir(parents=True, exist_ok=True)
            artifact: dict[str, object] = {
                "run_id": PREDICTION_ID,
                "path": str(target),
                "sha256": shadow._sha256(target),
                "decision_time": target_payload["decision_time"],
                "market_id": target_payload["market_id"],
                "unit": _json_object(json.dumps(
                    target_payload["operational_target_contract"],
                ))["unit"],
                "aggregate_target": target_payload["exposure_target"],
            }
            artifact.update(self.dry_artifact_patch)
            proposal: dict[str, object] = {
                "symbol": "BTC", "side": "BUY", "size": "0.00003",
                "price": "15000000", "notional_jpy": "450.00000",
            }
            proposal.update(self.dry_proposal_patch)
            payload: dict[str, object] = {
                "generated_at": "2026-08-26T00:00:01+00:00",
                "mode": "dry-run",
                "service_status": "OPEN",
                "artifact": artifact,
                "budget_jpy": "500",
                "reference_price": "15000000",
                "proposal": proposal,
                "skip_reason": None,
                "intent": {
                    "intent_id": "it-dry",
                    "correlation_id": target_payload["correlation_id"],
                    "state": "DRY_RUN_BLOCKED", "order_id": None,
                    "reason": "dry-run 发送边界拦截",
                },
                "endpoints": {
                    "read_touched": [],
                    "write_planned": [ORDER_ENDPOINT],
                    "write_touched": [],
                },
                "ledger_path": _option(parts, "--ledger"),
            }
            payload.update(self.dry_report_patch)
            report.write_text(json.dumps(payload), encoding="utf-8")
        return subprocess.CompletedProcess(
            parts, self.dry_returncode, "", "dry 进程异常",
        )

    def _paper(self, parts: list[str]) -> subprocess.CompletedProcess[str]:
        report = Path(_option(parts, "--report") or "")
        target = Path(_option(parts, "--target") or "")
        if self.paper_writes_report and self.paper_report is not None:
            report.parent.mkdir(parents=True, exist_ok=True)
            payload = dict(self.paper_report)
            payload["target_path"] = str(target)
            payload["target_sha256"] = shadow._sha256(target)
            target_payload = _json_object(target.read_bytes())
            if payload.get("status") != "duplicate_prediction":
                baseline = _paper_filled_report()
                for key in (
                    "decision_time", "valid_until", "correlation_id",
                    "market_id", "symbol", "exposure_target",
                    "risk_budget_jpy",
                ):
                    if payload.get(key) == baseline.get(key):
                        payload[key] = target_payload[key]
            ledger = self.execution / shadow.PAPER_LEDGER_ROOT / "execution/paper"
            payload["ledger_paths"] = {
                "intent_ledger": str(ledger / "intent_ledger.jsonl"),
                "position_ledger": str(ledger / "positions.jsonl"),
                "difference_ledger": str(ledger / "difference_ledger.jsonl"),
                "claim_ledger": str(ledger / "prediction_claims.jsonl"),
            }
            report.write_text(json.dumps(payload), encoding="utf-8")
        return subprocess.CompletedProcess(
            parts, self.paper_returncode, "", "paper 进程异常",
        )


@pytest.fixture
def chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeChain:
    fake = FakeChain(tmp_path)
    monkeypatch.setattr(shadow, "_run", fake.run)

    def repository_identity(
        repository: Path,
        name: str,
        expected: shadow.ExecutionIdentity | None = None,
        *,
        require_detached: bool = False,
        reject_untracked_scopes: Sequence[str] = (),
    ) -> shadow.ExecutionIdentity:
        del name, require_detached, reject_untracked_scopes
        if repository == fake.execution:
            dirty = fake.execution_dirty
            head = fake.execution_head
        elif repository == fake.runtime:
            dirty = fake.runtime_dirty
            head = fake.runtime_head
        elif repository == fake.code:
            dirty = fake.code_dirty
            head = fake.code_head
        else:
            raise AssertionError(f"未知假 Git 根: {repository}")
        if dirty:
            raise ValueError("代码仓含受跟踪未提交改动")
        current = shadow.ExecutionIdentity(repository, head)
        if expected is not None and current != expected:
            raise ValueError("代码仓 tracked-clean HEAD 已变化")
        return current

    def execution_identity(
        execution: Path,
        expected: shadow.ExecutionIdentity | None = None,
    ) -> shadow.ExecutionIdentity:
        return repository_identity(execution, "执行仓", expected)

    def tracked(repository: Path, name: str) -> tuple[shadow.FileIdentity, ...]:
        del name
        script_root = repository / "scripts"
        return tuple(
            shadow._stable_identity(path, "假跟踪文件")
            for path in sorted(script_root.glob("*.py"))
        )

    monkeypatch.setattr(shadow, "_repository_identity", repository_identity)
    monkeypatch.setattr(shadow, "_execution_identity", execution_identity)
    monkeypatch.setattr(shadow, "_git_tracked_identities", tracked)
    monkeypatch.setattr(shadow, "_runner_code_root", lambda: fake.code)
    monkeypatch.setattr(
        shadow, "_current_python_executable", lambda: fake.runner_python,
    )
    return fake


def _run_chain(chain: FakeChain, **overrides: object) -> dict[str, object]:
    overrides.setdefault("paper_enabled", False)
    return shadow.run_shadow(
        chain.repository, chain.runtime, chain.execution, PLAN_ID, MARKET_ID,
        code_root=chain.code,
        expected_code_head=chain.code_head,
        python_executable=chain.runner_python,
        expected_python_sha256=chain.expected_python_sha256,
        git_executable=chain.git_executable,
        expected_git_sha256=chain.expected_git_sha256,
        expected_execution_environment_tree_sha256=(
            chain.expected_environment_tree_sha256
        ),
        **overrides,  # type: ignore[arg-type]
    )


def _task_records(chain: FakeChain) -> list[dict[str, object]]:
    path = chain.execution / shadow.SHADOW_ROOT / "task.jsonl"
    return [
        _json_object(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _dry_report(chain: FakeChain) -> Path:
    return chain.execution / shadow.SHADOW_ROOT / "reports" / f"{PREDICTION_ID}.json"


def _dry_receipt(chain: FakeChain) -> Path:
    return chain.execution / shadow.SHADOW_ROOT / "receipts" / f"{PREDICTION_ID}.json"


def _paper_report(chain: FakeChain) -> Path:
    return chain.execution / shadow.PAPER_ROOT / "reports" / f"{PREDICTION_ID}.json"


def _paper_receipt(chain: FakeChain) -> Path:
    return chain.execution / shadow.PAPER_ROOT / "receipts" / f"{PREDICTION_ID}.json"


def _paper_from(summary: Mapping[str, object]) -> dict[str, object]:
    value = summary.get("paper")
    assert isinstance(value, dict)
    return {str(key): item for key, item in value.items()}


def _validate_paper_fixture(
    chain: FakeChain,
    report: Mapping[str, object],
) -> dict[str, object]:
    """不运行 paper executor，仅对 producer-shaped 报告执行冻结合同。"""
    summary = _run_chain(chain, paper_enabled=False)
    source_path = Path(str(summary["source_prediction_snapshot_path"]))
    source = shadow._stable_identity(source_path, "paper validator source")
    official = _json_object(source_path.read_bytes())
    config_payload = _json_object(
        (chain.execution / shadow.PAPER_CONFIG).read_bytes(),
    )
    config_contract = shadow._paper_config_contract(
        config_payload, market_id=MARKET_ID, symbol="BTC",
    )
    expectation = shadow._target_expectation(
        official,
        source,
        config_contract,
        plan_id=PLAN_ID,
        market_id=MARKET_ID,
        symbol="BTC",
        mode=shadow.PAPER_MODE,
        budget_jpy=None,
    )
    output = chain.execution / "paper-validator-targets"
    adapted = chain._adapt([
        "adapt_frozen_target.py",
        "--prediction", str(source_path),
        "--output-directory", str(output),
        "--config", str(chain.execution / shadow.PAPER_CONFIG),
        "--market-id", MARKET_ID,
        "--symbol", "BTC",
        "--mode", shadow.PAPER_MODE,
    ])
    adapted_payload = _json_object(adapted.stdout)
    target = shadow._stable_identity(
        Path(str(adapted_payload["path"])),
        "paper validator target",
        str(adapted_payload["sha256"]),
    )
    if report.get("status") == "duplicate_prediction":
        payload = dict(report)
    else:
        payload = _paper_filled_report()
        payload.update(report)
    target_payload = _json_object(target.path.read_bytes())
    payload["target_path"] = str(target.path)
    payload["target_sha256"] = target.sha256
    if report.get("status") != "duplicate_prediction":
        baseline = _paper_filled_report()
        for key in (
            "decision_time", "valid_until", "correlation_id", "market_id",
            "symbol", "exposure_target", "risk_budget_jpy",
        ):
            if payload.get(key) == baseline.get(key):
                payload[key] = target_payload[key]
    ledger = chain.execution / shadow.PAPER_LEDGER_ROOT / "execution/paper"
    payload["ledger_paths"] = {
        "intent_ledger": str(ledger / "intent_ledger.jsonl"),
        "position_ledger": str(ledger / "positions.jsonl"),
        "difference_ledger": str(ledger / "difference_ledger.jsonl"),
        "claim_ledger": str(ledger / "prediction_claims.jsonl"),
    }
    return shadow._validate_paper_report_bytes(
        _canonical(payload), target, expectation, chain.execution,
    )


def test_chain_uses_snapshot_and_commits_exact_receipts(chain: FakeChain) -> None:
    summary = _run_chain(chain)
    paper = _paper_from(summary)

    assert summary["status"] == "completed"
    assert summary["execution_environment_attestation"] == "partial"
    assert summary["intent_state"] == "DRY_RUN_BLOCKED"
    assert paper == {"status": "skipped", "reason": "--no-paper"}
    origin = chain.prediction_path.resolve()
    snapshot = Path(str(summary["source_prediction_snapshot_path"]))
    source_sha256 = hashlib.sha256(chain.official_bytes()).hexdigest()
    assert Path(str(summary["source_prediction_origin_path"])) == origin
    assert summary["source_prediction_origin_sha256"] == source_sha256
    assert summary["source_prediction_snapshot_sha256"] == source_sha256
    assert snapshot == (
        chain.execution / shadow.SOURCE_DIRECTORY / f"source-{source_sha256}.json"
    ).resolve()
    assert snapshot.read_bytes() == chain.official_bytes()

    adapters = chain.scripts("adapt_frozen_target.py")
    dry_calls = chain.scripts("run_dry_run_executor.py")
    paper_calls = chain.scripts("run_paper_executor.py")
    assert [_option(call, "--mode") for call in adapters] == ["dry-run"]
    assert all(_option(call, "--prediction") == str(snapshot) for call in adapters)
    assert paper_calls == []
    assert _option(dry_calls[0], "--source-prediction") == str(snapshot)
    assert _option(dry_calls[0], "--source-prediction-sha256") == source_sha256
    dry_env = chain.environments("run_dry_run_executor.py")[0]
    assert dry_env is not None
    assert dry_env["GUVOLU_MODE"] == "dry-run"

    expected_keys = {
        "schema_version", "kind", "commit_state", "status", "stage",
        "plan_id", "prediction_id", "source_origin", "source_snapshot",
        "target", "report", "config_origin", "config_snapshot",
        "runner_python", "git_executable", "execution_environment",
        "code_repository",
        "runtime_repository",
        "execution_repository",
    }
    for path, stage in ((_dry_receipt(chain), "dry-run"),):
        receipt = _json_object(path.read_bytes())
        assert set(receipt) == expected_keys
        assert receipt["schema_version"] == 6
        assert receipt["kind"] == shadow.RECEIPT_KIND
        assert receipt["commit_state"] == "committed"
        assert receipt["status"] == "succeeded"
        assert receipt["stage"] == stage
        assert receipt["plan_id"] == PLAN_ID
        assert receipt["prediction_id"] == PREDICTION_ID
        assert receipt["source_origin"] == {
            "path": str(origin), "sha256": source_sha256,
        }
        assert receipt["source_snapshot"] == {
            "path": str(snapshot), "sha256": source_sha256,
        }
        config = chain.execution / shadow.PAPER_CONFIG
        assert receipt["config_origin"] == {
            "path": str(config.resolve()), "sha256": shadow._sha256(config),
        }
        config_sha = shadow._sha256(config)
        config_snapshot = (
            chain.execution / shadow.CONFIG_SOURCE_DIRECTORY
            / f"config-{config_sha}.json"
        )
        assert receipt["config_snapshot"] == {
            "path": str(config_snapshot.resolve()), "sha256": config_sha,
        }
        assert receipt["runner_python"] == {
            "path": str(chain.runner_python.resolve()),
            "sha256": shadow._sha256(chain.runner_python),
        }
        assert receipt["git_executable"] == {
            "path": str(chain.git_executable.resolve()),
            "sha256": shadow._sha256(chain.git_executable),
        }
        python = chain.execution / ".venv/Scripts/python.exe"
        pyvenv = chain.execution / ".venv/pyvenv.cfg"
        environment = receipt["execution_environment"]
        assert isinstance(environment, dict)
        assert environment["attestation"] == "partial"
        assert environment["guard_strength"] == shadow.WINDOWS_GUARD_STRENGTH
        assert environment["python"] == {
            "path": str(python.resolve()),
            "sha256": shadow._sha256(python),
        }
        assert environment["pyvenv_config"] == {
            "path": str(pyvenv.resolve()),
            "sha256": shadow._sha256(pyvenv),
        }
        assert environment["file_count"] == 2
        assert isinstance(environment["total_bytes"], int)
        assert isinstance(environment["tree_sha256"], str)
        assert isinstance(environment["manifest"], dict)
        assert environment["pycache_sentinel"] == {
            "path": str((
                chain.execution / shadow.SHADOW_ROOT
                / "environment-manifests/pycache-disabled.sentinel"
            ).resolve()),
            "sha256": hashlib.sha256(
                shadow.PYCACHE_SENTINEL_BODY,
            ).hexdigest(),
        }
        empty_env = (
            chain.execution / shadow.SHADOW_ROOT / "environment-manifests"
            / f"child-env-{hashlib.sha256(shadow.EMPTY_CHILD_ENV_BODY).hexdigest()}.env"
        )
        assert environment["empty_child_env"] == {
            "path": str(empty_env.resolve()),
            "sha256": hashlib.sha256(shadow.EMPTY_CHILD_ENV_BODY).hexdigest(),
        }
        closures = environment["import_closures"]
        assert isinstance(closures, dict)
        assert set(closures) == {"code", "runtime", "execution"}
        for role, value in closures.items():
            assert isinstance(value, dict)
            assert value["role"] == role
            assert Path(str(value["root"])).is_dir()
            manifest_value = value["manifest"]
            assert isinstance(manifest_value, dict)
            manifest_path = Path(str(manifest_value["path"]))
            assert manifest_path.is_file()
            assert shadow._sha256(manifest_path) == manifest_value["sha256"]
            assert isinstance(value["tree_sha256"], str)
        assert receipt["code_repository"] == {
            "path": str(chain.code.resolve()),
            "head_commit": chain.code_head,
        }
        assert receipt["runtime_repository"] == {
            "path": str(chain.runtime.resolve()),
            "head_commit": chain.runtime_head,
        }
        assert receipt["execution_repository"] == {
            "path": str(chain.execution.resolve()),
            "head_commit": chain.execution_head,
        }
    assert summary["receipt_sha256"] == shadow._sha256(_dry_receipt(chain))
    assert not _paper_receipt(chain).exists()


def test_predictor_return_then_source_flip_fails_before_snapshot(
    chain: FakeChain, monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = shadow._stable_file_bytes
    flipped = False

    def flip_before_capture(
        path: Path, name: str, *, allow_hardlinks: bool = False,
    ) -> bytes:
        nonlocal flipped
        if name == "官方冻结预测" and not flipped:
            path.write_text('{"flipped":true}\n', encoding="utf-8")
            flipped = True
        return original(path, name, allow_hardlinks=allow_hardlinks)

    monkeypatch.setattr(shadow, "_stable_file_bytes", flip_before_capture)

    with pytest.raises(ValueError, match="predictor 声明 SHA-256 不符"):
        _run_chain(chain)

    assert chain.scripts("adapt_frozen_target.py") == []
    assert chain.scripts("run_dry_run_executor.py") == []
    source_root = chain.execution / shadow.SOURCE_DIRECTORY
    assert source_root.is_dir()
    assert list(source_root.iterdir()) == []
    assert not _dry_report(chain).exists()
    assert not _dry_receipt(chain).exists()


def test_origin_change_after_capture_cannot_change_execution_snapshot(
    chain: FakeChain,
) -> None:
    chain.mutate_origin_on_adapter = True

    summary = _run_chain(chain)
    paper = _paper_from(summary)
    snapshot = Path(str(summary["source_prediction_snapshot_path"]))

    assert chain.origin_mutated
    assert chain.prediction_path.read_bytes() != snapshot.read_bytes()
    assert snapshot.read_bytes() == chain.official_bytes()
    for call in (
        chain.scripts("adapt_frozen_target.py")
        + chain.scripts("run_dry_run_executor.py")
        + chain.scripts("run_paper_executor.py")
    ):
        option = (
            "--prediction"
            if _script_name(call) == "adapt_frozen_target.py"
            else "--source-prediction"
        )
        assert _option(call, option) == str(snapshot)
    assert summary["status"] == "completed"
    assert paper == {"status": "skipped", "reason": "--no-paper"}


@pytest.mark.parametrize("case", ["sha-shape", "sha", "filename"])
def test_adapter_hash_and_filename_mismatch_fail_before_executor(
    chain: FakeChain, case: str,
) -> None:
    if case == "sha-shape":
        chain.adapter_sha_override = "not-a-sha256"
        message = "不是小写 SHA-256"
    elif case == "sha":
        chain.adapter_sha_override = "0" * 64
        message = "SHA-256 已变化"
    else:
        chain.adapter_bad_filename = True
        message = "文件名不是声明散列"

    with pytest.raises(ValueError, match=message):
        _run_chain(chain)

    assert len(chain.scripts("adapt_frozen_target.py")) == 1
    assert chain.scripts("run_dry_run_executor.py") == []
    assert chain.scripts("run_paper_executor.py") == []
    assert not _dry_report(chain).exists()
    assert not _dry_receipt(chain).exists()


@pytest.mark.parametrize("artifact", ["source", "target", "report"])
def test_dry_reuse_fails_closed_when_any_artifact_changes(
    chain: FakeChain, artifact: str,
) -> None:
    first = _run_chain(chain, paper_enabled=False)
    if artifact == "source":
        path = Path(str(first["source_prediction_snapshot_path"]))
    elif artifact == "target":
        path = Path(str(first["target_path"]))
    else:
        path = Path(str(first["report_path"]))
    path.write_bytes(path.read_bytes() + b"\n")

    with pytest.raises(ValueError):
        _run_chain(chain, paper_enabled=False)

    assert len(chain.scripts("run_dry_run_executor.py")) == 1
    assert len(chain.scripts("adapt_frozen_target.py")) == 1
    assert _task_records(chain)[-1]["status"] == "failed"


def test_explicit_paper_request_fails_before_any_mutation(chain: FakeChain) -> None:
    before = {
        path.relative_to(chain.execution).as_posix(): path.read_bytes()
        for path in chain.execution.rglob("*")
        if path.is_file()
    }

    with pytest.raises(ValueError, match="paper 执行已禁用"):
        _run_chain(chain, paper_enabled=True)

    after = {
        path.relative_to(chain.execution).as_posix(): path.read_bytes()
        for path in chain.execution.rglob("*")
        if path.is_file()
    }
    assert after == before
    assert chain.calls == []
    assert not (chain.execution / shadow.SHADOW_ROOT).exists()
    assert not (chain.execution / shadow.PAPER_ROOT).exists()


def test_dry_nonzero_with_report_leaves_no_receipt_and_blocks_rerun(
    chain: FakeChain,
) -> None:
    chain.dry_returncode = 2

    with pytest.raises(RuntimeError, match=r"dry-run 失败\(2\)"):
        _run_chain(chain, paper_enabled=False)

    assert _dry_report(chain).is_file()
    assert not _dry_receipt(chain).exists()
    chain.dry_returncode = 0
    with pytest.raises(ValueError, match="单边存在"):
        _run_chain(chain, paper_enabled=False)
    assert len(chain.scripts("run_dry_run_executor.py")) == 1
    assert len(chain.scripts("adapt_frozen_target.py")) == 1


@pytest.mark.parametrize("missing", ["report", "receipt"])
def test_dry_report_receipt_single_side_is_not_reused(
    chain: FakeChain, missing: str,
) -> None:
    _run_chain(chain, paper_enabled=False)
    path = _dry_report(chain) if missing == "report" else _dry_receipt(chain)
    path.unlink()

    with pytest.raises(ValueError, match="单边存在"):
        _run_chain(chain, paper_enabled=False)

    assert len(chain.scripts("run_dry_run_executor.py")) == 1
    assert len(chain.scripts("adapt_frozen_target.py")) == 1


def test_valid_receipts_reuse_without_adapter_or_executor(chain: FakeChain) -> None:
    first = _run_chain(chain)
    second = _run_chain(chain)
    assert first["status"] == "completed"
    assert second["status"] == "reused"
    assert second["paper"] == {"status": "skipped", "reason": "--no-paper"}
    assert second["target_sha256"] == first["target_sha256"]
    assert second["report_sha256"] == first["report_sha256"]
    assert len(chain.scripts("adapt_frozen_target.py")) == 1
    assert len(chain.scripts("run_dry_run_executor.py")) == 1
    assert len(chain.scripts("run_paper_executor.py")) == 0
    assert [record["status"] for record in _task_records(chain)] == [
        "completed", "reused",
    ]


def test_legacy_v5_receipt_is_not_migrated_or_reused(chain: FakeChain) -> None:
    receipt_path = _dry_receipt(chain)
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_bytes(_canonical({"schema_version": 5}))
    before = {
        path.relative_to(chain.execution).as_posix(): path.read_bytes()
        for path in chain.execution.rglob("*")
        if path.is_file()
    }

    with pytest.raises(ValueError, match="旧版回执不迁移、不复用"):
        _run_chain(chain, paper_enabled=False)

    after = {
        path.relative_to(chain.execution).as_posix(): path.read_bytes()
        for path in chain.execution.rglob("*")
        if path.is_file()
    }
    assert after == before
    assert chain.calls == []
    assert not (chain.execution / shadow.CONFIG_SOURCE_DIRECTORY).exists()
    assert not (chain.execution / shadow.PAPER_ROOT).exists()


def test_shadow_forces_dry_run_mode_over_inherited_live_environment(
    chain: FakeChain,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GUVOLU_MODE", "live")

    _run_chain(chain)

    environments = chain.environments("run_dry_run_executor.py")
    assert len(environments) == 1
    assert environments[0] is not None
    assert environments[0]["GUVOLU_MODE"] == "dry-run"
    assert os.environ["GUVOLU_MODE"] == "live"


def test_business_children_ignore_hostile_process_and_dotenv_state(
    chain: FakeChain,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hostile = {
        "GUVOLU_MODE": "live",
        "GUVOLU_DATA_ROOT": "outside",
        "GMO_API_KEY": "secret-key",
        "GMO_API_SECRET": "secret-value",
        "HTTP_PROXY": "http://attacker.invalid",
        "HTTPS_PROXY": "http://attacker.invalid",
        "PYTHONPATH": "malicious-python-path",
        "PYTHONHOME": "malicious-python-home",
        "GIT_DIR": "malicious-git-dir",
        "AWS_ACCESS_KEY_ID": "credential",
    }
    for key, value in hostile.items():
        monkeypatch.setenv(key, value)
    (chain.execution / ".env").write_text(
        "GUVOLU_MODE=live\nGMO_API_KEY=dotenv-secret\n"
        "HTTP_PROXY=http://dotenv.invalid\n",
        encoding="utf-8",
    )

    _run_chain(chain, paper_enabled=False)

    os_allowlist = {
        "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP", "TMPDIR",
    }
    for script in (
        "manage_frozen_forward.py", "adapt_frozen_target.py",
        "run_dry_run_executor.py",
    ):
        environments = chain.environments(script)
        assert environments
        for environment in environments:
            assert environment is not None
            assert set(environment) <= os_allowlist | {"GUVOLU_MODE"}
            for key in hostile:
                if key != "GUVOLU_MODE":
                    assert key not in environment
            if script == "run_dry_run_executor.py":
                assert environment.get("GUVOLU_MODE") == "dry-run"
            else:
                assert "GUVOLU_MODE" not in environment

    dry_command = chain.scripts("run_dry_run_executor.py")[0]
    env_file_text = _option(dry_command, "--env-file")
    assert env_file_text is not None
    env_file = Path(env_file_text)
    assert env_file.read_bytes() == shadow.EMPTY_CHILD_ENV_BODY
    assert env_file.stat().st_nlink == 1
    receipt = _json_object(_dry_receipt(chain).read_bytes())
    execution_environment = receipt["execution_environment"]
    assert isinstance(execution_environment, dict)
    assert execution_environment["empty_child_env"] == {
        "path": str(env_file.resolve()),
        "sha256": hashlib.sha256(shadow.EMPTY_CHILD_ENV_BODY).hexdigest(),
    }


def test_stale_prediction_fails_before_target_adaptation(chain: FakeChain) -> None:
    chain.prediction_decision_time = datetime.now(UTC) - timedelta(minutes=46)

    with pytest.raises(ValueError, match="冻结预测过期"):
        _run_chain(chain)

    assert chain.scripts("adapt_frozen_target.py") == []
    assert _task_records(chain)[-1]["status"] == "failed"


@pytest.mark.parametrize(
    ("patch", "message"),
    [
        ({"endpoints": {
            "read_touched": [], "write_planned": [],
            "write_touched": [ORDER_ENDPOINT],
        }}, "触及了写端点"),
        ({"mode": "dry-run"}, "差异行身份不符"),
        ({"prediction_id": "frozen-forward-prediction-other"}, "预测身份不符"),
    ],
)
def test_invalid_paper_report_has_no_success_receipt(
    chain: FakeChain, patch: dict[str, object], message: str,
) -> None:
    report = _paper_filled_report()
    report.update(patch)

    with pytest.raises(ValueError, match=message):
        _validate_paper_fixture(chain, report)
    assert not _paper_receipt(chain).exists()


def test_reconciliation_outcome_has_no_success_receipt(chain: FakeChain) -> None:
    report = _paper_filled_report()
    report["status"] = "needs_reconciliation"

    with pytest.raises(ValueError, match="不在允许终态"):
        _validate_paper_fixture(chain, report)
    assert not _paper_receipt(chain).exists()


def test_duplicate_paper_report_is_a_valid_terminal_shape(chain: FakeChain) -> None:
    report = _validate_paper_fixture(chain, _duplicate_paper_report())

    assert report["status"] == "duplicate_prediction"
    assert not _paper_receipt(chain).exists()


def test_no_paper_skips_paper_stage_and_receipt(chain: FakeChain) -> None:
    summary = _run_chain(chain, paper_enabled=False)

    assert summary["status"] == "completed"
    assert summary["paper"] == {"status": "skipped", "reason": "--no-paper"}
    assert chain.scripts("run_paper_executor.py") == []
    assert not _paper_report(chain).exists()
    assert not _paper_receipt(chain).exists()


def test_python_api_defaults_to_no_paper(chain: FakeChain) -> None:
    summary = shadow.run_shadow(
        chain.repository,
        chain.runtime,
        chain.execution,
        PLAN_ID,
        MARKET_ID,
        code_root=chain.code,
        expected_code_head=chain.code_head,
        python_executable=chain.runner_python,
        expected_python_sha256=chain.expected_python_sha256,
        git_executable=chain.git_executable,
        expected_git_sha256=chain.expected_git_sha256,
        expected_execution_environment_tree_sha256=(
            chain.expected_environment_tree_sha256
        ),
    )

    assert summary["paper"] == {"status": "skipped", "reason": "--no-paper"}
    assert chain.scripts("run_paper_executor.py") == []
    assert not _paper_receipt(chain).exists()


def test_receipt_extra_field_invalidates_reuse(chain: FakeChain) -> None:
    _run_chain(chain, paper_enabled=False)
    receipt = _json_object(_dry_receipt(chain).read_bytes())
    receipt["unexpected"] = True
    _dry_receipt(chain).write_bytes(_canonical(receipt))

    with pytest.raises(ValueError, match="精确合同"):
        _run_chain(chain, paper_enabled=False)

    assert len(chain.scripts("run_dry_run_executor.py")) == 1


def test_plan_id_is_rejected_before_any_path_or_subprocess(chain: FakeChain) -> None:
    with pytest.raises(ValueError, match="规范冻结标识"):
        shadow.run_shadow(
            chain.repository,
            chain.runtime,
            chain.execution,
            "frozen-forward-plan-../escape",
            MARKET_ID,
            code_root=chain.code,
            expected_code_head=chain.code_head,
            python_executable=chain.runner_python,
            expected_python_sha256=chain.expected_python_sha256,
            git_executable=chain.git_executable,
            expected_git_sha256=chain.expected_git_sha256,
            expected_execution_environment_tree_sha256=(
                chain.expected_environment_tree_sha256
            ),
        )

    assert chain.calls == []
    assert not (chain.execution / shadow.SHADOW_ROOT).exists()


def test_prediction_id_is_rejected_before_artifact_paths(chain: FakeChain) -> None:
    chain.prediction_id_override = "frozen-forward-prediction-../escape"

    with pytest.raises(ValueError, match="规范冻结标识"):
        _run_chain(chain)

    assert chain.scripts("adapt_frozen_target.py") == []
    assert chain.scripts("run_dry_run_executor.py") == []


def test_predictor_path_traversal_is_rejected(chain: FakeChain) -> None:
    outside = chain.runtime.parent / "outside-prediction.json"
    outside.write_bytes(chain.official_bytes())
    chain.prediction_path_override = outside

    with pytest.raises(ValueError, match="越出受管根"):
        _run_chain(chain)

    assert chain.scripts("adapt_frozen_target.py") == []


def test_adapter_path_outside_target_root_is_rejected(chain: FakeChain) -> None:
    chain.adapter_path_override = chain.execution.parent / "escaped-target.json"

    with pytest.raises(ValueError, match="越出受管目录"):
        _run_chain(chain)

    assert chain.scripts("run_dry_run_executor.py") == []


def test_prepositioned_target_junction_cannot_write_outside(chain: FakeChain) -> None:
    managed_parent = chain.execution / "data/execution"
    managed_parent.mkdir(parents=True)
    outside = chain.execution.parent / "outside-targets"
    outside.mkdir()
    alias = managed_parent / "targets"
    _make_directory_alias(outside, alias)
    try:
        with pytest.raises(ValueError, match="junction|reparse|目录别名"):
            _run_chain(chain)
        assert list(outside.iterdir()) == []
        assert chain.scripts("run_dry_run_executor.py") == []
    finally:
        _remove_directory_alias(alias)


def test_prepositioned_report_junction_is_rejected_before_refresh(
    chain: FakeChain,
) -> None:
    shadow_root = chain.execution / shadow.SHADOW_ROOT
    shadow_root.mkdir(parents=True)
    outside = chain.execution.parent / "outside-reports"
    outside.mkdir()
    alias = shadow_root / "reports"
    _make_directory_alias(outside, alias)
    try:
        with pytest.raises(ValueError, match="junction|reparse|目录别名"):
            _run_chain(chain)
        assert chain.calls == []
        assert list(outside.iterdir()) == []
    finally:
        _remove_directory_alias(alias)


@pytest.mark.parametrize("artifact", ["origin", "snapshot", "target", "report", "receipt"])
def test_any_hardlinked_execution_evidence_is_rejected(
    chain: FakeChain, artifact: str,
) -> None:
    first = _run_chain(chain, paper_enabled=False)
    paths = {
        "origin": Path(str(first["source_prediction_origin_path"])),
        "snapshot": Path(str(first["source_prediction_snapshot_path"])),
        "target": Path(str(first["target_path"])),
        "report": Path(str(first["report_path"])),
        "receipt": Path(str(first["receipt_path"])),
    }
    alias = paths[artifact].parent / f"hardlink-{artifact}.json"
    os.link(paths[artifact], alias)
    try:
        with pytest.raises(ValueError, match="常规文件|读取期间发生变化"):
            _run_chain(chain, paper_enabled=False)
        assert len(chain.scripts("run_dry_run_executor.py")) == 1
    finally:
        alias.unlink()


def test_receipt_single_read_digest_decode_closes_aba_window(
    chain: FakeChain, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _run_chain(chain, paper_enabled=False)
    receipt = _dry_receipt(chain)
    original = receipt.read_bytes()
    changed = original.replace(b'"status":"succeeded"', b'"status":"xucceeded"')
    assert len(changed) == len(original)
    stable = shadow._stable_file_bytes
    injected = False

    def mutate_after_first_read(
        path: Path, name: str, *, allow_hardlinks: bool = False,
    ) -> bytes:
        nonlocal injected
        body = stable(path, name, allow_hardlinks=allow_hardlinks)
        if name == "dry-run committed receipt" and not injected:
            path.write_bytes(changed)
            injected = True
        return body

    monkeypatch.setattr(shadow, "_stable_file_bytes", mutate_after_first_read)

    with pytest.raises(ValueError, match="SHA-256 已变化"):
        _run_chain(chain, paper_enabled=False)
    assert injected


def test_reuse_rechecks_origin_at_the_end(
    chain: FakeChain, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _run_chain(chain, paper_enabled=False)
    stable = shadow._stable_file_bytes
    injected = False

    def mutate_origin_after_check(
        path: Path, name: str, *, allow_hardlinks: bool = False,
    ) -> bytes:
        nonlocal injected
        body = stable(path, name, allow_hardlinks=allow_hardlinks)
        if name == "receipt 来源原件" and not injected:
            path.write_bytes(body + b" ")
            injected = True
        return body

    monkeypatch.setattr(shadow, "_stable_file_bytes", mutate_origin_after_check)

    with pytest.raises(ValueError, match="SHA-256 已变化"):
        _run_chain(chain, paper_enabled=False)
    assert injected


def test_config_drift_invalidates_committed_receipt(chain: FakeChain) -> None:
    _run_chain(chain, paper_enabled=False)
    config = chain.execution / shadow.PAPER_CONFIG
    payload = _json_object(config.read_bytes())
    payload["no_trade_band"] = "0.02"
    config.write_bytes(_canonical(payload))

    with pytest.raises(ValueError, match="配置或代码仓身份不符"):
        _run_chain(chain, paper_enabled=False)
    assert len(chain.scripts("run_dry_run_executor.py")) == 1


@pytest.mark.parametrize(
    "relative_path",
    [".venv/Scripts/python.exe", ".venv/pyvenv.cfg"],
)
def test_venv_bootstrap_drift_invalidates_committed_receipt(
    chain: FakeChain, relative_path: str,
) -> None:
    _run_chain(chain, paper_enabled=False)
    path = chain.execution / relative_path
    path.write_bytes(path.read_bytes() + b"drift\n")

    with pytest.raises(ValueError, match="执行 venv 清单与注册值不符"):
        _run_chain(chain, paper_enabled=False)
    assert len(chain.scripts("run_dry_run_executor.py")) == 1


def test_receipt_cannot_upgrade_partial_environment_attestation(
    chain: FakeChain,
) -> None:
    _run_chain(chain, paper_enabled=False)
    receipt = _json_object(_dry_receipt(chain).read_bytes())
    environment = receipt["execution_environment"]
    assert isinstance(environment, dict)
    environment["attestation"] = "complete"
    _dry_receipt(chain).write_bytes(_canonical(receipt))

    with pytest.raises(ValueError, match="环境证明级别"):
        _run_chain(chain, paper_enabled=False)


def test_venv_python_hardlink_is_guarded_when_manifest_is_unchanged(
    chain: FakeChain,
    tmp_path: Path,
) -> None:
    python = chain.execution / ".venv/Scripts/python.exe"
    alias = tmp_path / "python-alias.exe"
    os.link(python, alias)
    try:
        summary = _run_chain(chain, paper_enabled=False)
        assert summary["status"] == "completed"
        assert len(chain.scripts("adapt_frozen_target.py")) == 1
        assert len(chain.scripts("run_dry_run_executor.py")) == 1
    finally:
        alias.unlink()


def test_venv_drift_during_child_blocks_receipt(
    chain: FakeChain, monkeypatch: pytest.MonkeyPatch,
) -> None:
    mutated = False

    def run_and_mutate(
        command: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal mutated
        result = chain.run(command, cwd=cwd, env=env)
        if _script_name(command) == "run_dry_run_executor.py":
            python = chain.execution / ".venv/Scripts/python.exe"
            python.write_bytes(python.read_bytes() + b"during-child\n")
            mutated = True
        return result

    monkeypatch.setattr(shadow, "_run", run_and_mutate)

    with pytest.raises(PermissionError):
        _run_chain(chain, paper_enabled=False)
    assert not mutated
    assert not _dry_report(chain).exists()
    assert not _dry_receipt(chain).exists()


def test_execution_head_or_tracked_dirty_state_fails_closed(chain: FakeChain) -> None:
    _run_chain(chain, paper_enabled=False)
    chain.execution_head = "f" * 40
    with pytest.raises(ValueError, match="配置或代码仓身份不符"):
        _run_chain(chain, paper_enabled=False)

    chain.execution_dirty = True
    with pytest.raises(ValueError, match="未提交改动"):
        _run_chain(chain, paper_enabled=False)
    assert len(chain.scripts("run_dry_run_executor.py")) == 1


def test_config_origin_aba_cannot_reach_children(
    chain: FakeChain, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_origin = chain.execution / shadow.PAPER_CONFIG
    original = config_origin.read_bytes()
    changed = _json_object(original)
    changed["no_trade_band"] = "0.02"
    observed_snapshot: Path | None = None

    def run_with_origin_aba(
        command: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal observed_snapshot
        parts = [str(item) for item in command]
        if _script_name(parts) == "adapt_frozen_target.py":
            observed_snapshot = Path(_option(parts, "--config") or "")
            assert observed_snapshot != config_origin
            assert observed_snapshot.read_bytes() == original
            config_origin.write_bytes(_canonical(changed))
            try:
                return chain.run(command, cwd=cwd, env=env)
            finally:
                config_origin.write_bytes(original)
        return chain.run(command, cwd=cwd, env=env)

    monkeypatch.setattr(shadow, "_run", run_with_origin_aba)

    summary = _run_chain(chain, paper_enabled=False)

    assert summary["status"] == "completed"
    assert observed_snapshot is not None
    receipt = _json_object(_dry_receipt(chain).read_bytes())
    assert receipt["config_origin"] != receipt["config_snapshot"]


@pytest.mark.skipif(os.name != "nt", reason="Windows no-share guard contract")
@pytest.mark.parametrize(
    ("artifact", "script"),
    [
        ("source_snapshot", "adapt_frozen_target.py"),
        ("config_snapshot", "adapt_frozen_target.py"),
        ("target", "run_dry_run_executor.py"),
        ("execution_python", "adapt_frozen_target.py"),
        ("execution_tracked", "adapt_frozen_target.py"),
        ("runtime_tracked", "manage_frozen_forward.py"),
        ("code_tracked", "manage_frozen_forward.py"),
        ("runner_python", "manage_frozen_forward.py"),
    ],
)
def test_windows_child_window_aba_is_blocked_by_native_handles(
    chain: FakeChain,
    monkeypatch: pytest.MonkeyPatch,
    artifact: str,
    script: str,
) -> None:
    attempted = False

    def run_with_aba(
        command: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal attempted
        parts = [str(item) for item in command]
        if _script_name(parts) != script:
            return chain.run(command, cwd=cwd, env=env)
        candidates = {
            "source_snapshot": Path(_option(parts, "--prediction") or ""),
            "config_snapshot": Path(_option(parts, "--config") or ""),
            "target": Path(_option(parts, "--target") or ""),
            "execution_python": chain.execution / ".venv/Scripts/python.exe",
            "execution_tracked": chain.execution / "scripts/execution.py",
            "runtime_tracked": (
                chain.runtime / "scripts/manage_frozen_forward.py"
            ),
            "code_tracked": chain.code / "scripts/runner.py",
            "runner_python": chain.runner_python,
        }
        path = candidates[artifact]
        original = path.read_bytes()
        attempted = True
        path.write_bytes(original + b"aba\n")
        path.write_bytes(original)
        return chain.run(command, cwd=cwd, env=env)

    monkeypatch.setattr(shadow, "_run", run_with_aba)

    with pytest.raises(PermissionError):
        _run_chain(chain, paper_enabled=False)

    assert attempted
    assert not _dry_receipt(chain).exists()


def test_runner_python_drift_invalidates_receipt(chain: FakeChain) -> None:
    _run_chain(chain, paper_enabled=False)
    chain.runner_python.write_bytes(b"fixture-runner-python-v2\n")

    with pytest.raises(ValueError, match="runner Python SHA-256 已变化"):
        _run_chain(chain, paper_enabled=False)


def test_registered_git_executable_drift_fails_before_reuse(
    chain: FakeChain,
) -> None:
    _run_chain(chain, paper_enabled=False)
    chain.git_executable.write_bytes(b"fixture-git-v2\n")

    with pytest.raises(ValueError, match="Git 执行文件 SHA-256 已变化"):
        _run_chain(chain, paper_enabled=False)

    assert len(chain.scripts("run_dry_run_executor.py")) == 1


def test_registered_execution_environment_manifest_is_a_start_gate(
    chain: FakeChain,
) -> None:
    chain.expected_environment_tree_sha256 = "0" * 64

    with pytest.raises(ValueError, match="执行 venv 清单与注册值不符"):
        _run_chain(chain, paper_enabled=False)

    assert chain.scripts("manage_frozen_forward.py") == []
    assert chain.scripts("adapt_frozen_target.py") == []


@pytest.mark.parametrize(
    ("patch", "lineage_patch", "message"),
    [
        ({"market_id": "mkt__wrong"}, {}, "身份或调用参数不符"),
        ({"symbol": "ETH"}, {}, "身份或调用参数不符"),
        ({"exposure_target": 1.1}, {}, "exposure_target 不符"),
        ({"risk_budget_jpy": "501"}, {}, "risk_budget_jpy 不符"),
        ({"correlation_id": "co-wrong"}, {}, "身份或调用参数不符"),
        ({}, {"prediction_id": "wrong"}, "lineage 身份不符"),
    ],
)
def test_adapter_target_wrong_economic_lineage_is_rejected(
    chain: FakeChain,
    patch: dict[str, object],
    lineage_patch: dict[str, object],
    message: str,
) -> None:
    chain.target_patch = patch
    chain.target_lineage_patch = lineage_patch

    with pytest.raises(ValueError, match=message):
        _run_chain(chain, paper_enabled=False)

    assert chain.scripts("run_dry_run_executor.py") == []
    assert not _dry_receipt(chain).exists()


def test_same_prediction_concurrency_runs_each_stage_once(chain: FakeChain) -> None:
    body = chain.official_bytes()
    chain.prediction_path.parent.mkdir(parents=True)
    chain.prediction_path.write_bytes(body)
    digest = hashlib.sha256(body).hexdigest()
    snapshot = (
        chain.execution / shadow.SOURCE_DIRECTORY / f"source-{digest}.json"
    )
    snapshot.parent.mkdir(parents=True)
    snapshot.write_bytes(body)
    chain.rewrite_prediction = False
    chain.dry_delay_seconds = 0.1
    barrier = threading.Barrier(2)
    results: list[dict[str, object]] = []
    failures: list[BaseException] = []

    def worker() -> None:
        try:
            barrier.wait()
            results.append(_run_chain(chain))
        except BaseException as exc:
            failures.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not failures
    assert len(results) == 2
    assert len(chain.scripts("run_dry_run_executor.py")) == 1
    assert len(chain.scripts("run_paper_executor.py")) == 0
    assert len(chain.scripts("adapt_frozen_target.py")) == 1


@pytest.mark.parametrize("stage", ["dry", "paper"])
def test_reports_reject_non_allowlisted_read_endpoints(
    chain: FakeChain, stage: str,
) -> None:
    if stage == "dry":
        chain.dry_report_patch = {
            "endpoints": {
                "read_touched": [ORDER_ENDPOINT],
                "write_planned": [ORDER_ENDPOINT],
                "write_touched": [],
            },
        }
        with pytest.raises(ValueError, match="非允许 GET 端点"):
            _run_chain(chain, paper_enabled=False)
    else:
        report = _paper_filled_report()
        report["endpoints"] = {
            "read_touched": [ORDER_ENDPOINT],
            "write_planned": [],
            "write_touched": [],
        }
        with pytest.raises(ValueError, match="非允许 GET 端点"):
            _validate_paper_fixture(chain, report)


@pytest.mark.parametrize(
    ("location", "field", "value"),
    [
        ("artifact", "market_id", "mkt__wrong"),
        ("artifact", "unit", "contracts"),
        ("artifact", "aggregate_target", -999),
        ("proposal", "symbol", "ETH"),
    ],
)
def test_dry_report_wrong_economic_lineage_has_no_receipt(
    chain: FakeChain, location: str, field: str, value: object,
) -> None:
    if location == "artifact":
        chain.dry_artifact_patch[field] = value
    else:
        chain.dry_proposal_patch[field] = value

    with pytest.raises(ValueError, match="经济血缘|提案品种"):
        _run_chain(chain, paper_enabled=False)

    assert not _dry_receipt(chain).exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("market_id", "mkt__wrong"),
        ("symbol", "ETH"),
        ("correlation_id", "co-wrong"),
    ],
)
def test_paper_report_wrong_economic_lineage_has_no_receipt(
    chain: FakeChain, field: str, value: object,
) -> None:
    report = _paper_filled_report()
    report[field] = value

    with pytest.raises(ValueError, match="经济血缘与目标不符"):
        _validate_paper_fixture(chain, report)
    assert not _paper_receipt(chain).exists()


@pytest.mark.parametrize("stage", ["dry", "paper"])
def test_unknown_terminal_state_has_no_success_receipt(
    chain: FakeChain, stage: str,
) -> None:
    if stage == "dry":
        intent = {
            "intent_id": "it-dry", "correlation_id": "co-dry",
            "state": "DRY_RUN_REJECTED", "order_id": None,
            "reason": "wrong legacy state",
        }
        chain.dry_report_patch = {"intent": intent}
        with pytest.raises(ValueError, match="模拟拦截终态关系不符"):
            _run_chain(chain, paper_enabled=False)
        assert not _dry_receipt(chain).exists()
    else:
        report = _paper_filled_report()
        report["status"] = "PAPER_PARTIAL"
        with pytest.raises(ValueError, match="不在允许终态"):
            _validate_paper_fixture(chain, report)
        assert not _paper_receipt(chain).exists()


def test_dry_zero_proposal_producer_shape_is_receipted(chain: FakeChain) -> None:
    chain.dry_report_patch = {
        "proposal": None,
        "skip_reason": "目标为零，无需委托",
        "intent": None,
        "endpoints": {
            "read_touched": [], "write_planned": [], "write_touched": [],
        },
    }

    summary = _run_chain(chain, paper_enabled=False)

    assert summary["status"] == "completed"
    assert summary["intent_state"] is None
    assert _dry_receipt(chain).is_file()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("position_after", "-0.00001", "不得为负"),
        ("exposure_target", 1.1, "与目标不符"),
        ("target_notional_jpy", "201", "目标数量价格不符"),
    ],
)
def test_paper_report_rejects_long_only_or_numeric_contradictions(
    chain: FakeChain, field: str, value: object, message: str,
) -> None:
    report = _paper_filled_report()
    report[field] = value

    with pytest.raises(ValueError, match=message):
        _validate_paper_fixture(chain, report)
    assert not _paper_receipt(chain).exists()


def test_report_recursively_rejects_nonfinite_overlay(chain: FakeChain) -> None:
    report = _paper_filled_report()
    report["overlay"] = {"nested": [{"value": float("nan")}]}

    with pytest.raises(ValueError, match="非有限 JSON 数值"):
        _validate_paper_fixture(chain, report)


def test_paper_skip_reason_must_be_a_real_producer_reason(chain: FakeChain) -> None:
    report = _paper_filled_report()
    report.update({
        "status": "skipped",
        "position_after": "0",
        "intent": None,
        "fill": None,
        "cost": None,
        "fee": None,
    })
    raw_delta = report["delta"]
    assert isinstance(raw_delta, dict)
    delta = {str(key): item for key, item in raw_delta.items()}
    delta["proposal"] = None
    delta["skip_reason"] = "invented skip"
    report["delta"] = delta

    with pytest.raises(ValueError, match="skipped 字段关系不符"):
        _validate_paper_fixture(chain, report)


@pytest.mark.parametrize(
    "outcome",
    ["book_unavailable", "skipped", "sell_exceeds_position", "PAPER_REJECTED"],
)
def test_other_allowed_paper_terminal_contracts(
    chain: FakeChain, outcome: str,
) -> None:
    report = _paper_filled_report()
    report["status"] = outcome
    report["position_after"] = "0"
    report["intent"] = None
    report["fill"] = None
    report["cost"] = None
    report["fee"] = None
    if outcome == "book_unavailable":
        report["reference_price"] = None
        report["target_notional_jpy"] = None
        report["delta"] = None
        report["book_error"] = "盘口不可用"
    elif outcome == "skipped":
        raw_delta = report["delta"]
        assert isinstance(raw_delta, dict)
        delta = {str(key): item for key, item in raw_delta.items()}
        delta["proposal"] = None
        delta["skip_reason"] = "差分名义在不交易带内，无需委托"
        report["delta"] = delta
    elif outcome == "sell_exceeds_position":
        report["position_before"] = "0.00002"
        report["position_after"] = "0.00002"
        report["delta"] = {
            "desired_size": "0.00001333333333333333333333333333",
            "position_size": "0.00002",
            "delta_size": "-0.00000666666666666666666666666667",
            "proposal": {
                "symbol": "BTC", "side": "SELL", "size": "0.00003",
                "price": "15000000", "notional_jpy": "450.00000",
            },
            "skip_reason": None,
        }
    else:
        raw_intent = _paper_filled_report()["intent"]
        assert isinstance(raw_intent, dict)
        intent = {str(key): item for key, item in raw_intent.items()}
        intent["state"] = "PAPER_REJECTED"
        intent["reason"] = "paper 守卫拒绝"
        report["intent"] = intent
    validated = _validate_paper_fixture(chain, report)

    assert validated["status"] == outcome


def test_tick_rounded_fill_still_requires_bound_cost_provenance(
    chain: FakeChain,
) -> None:
    report = _paper_filled_report()
    reference = Decimal("15000000.5")
    desired = Decimal("0.4") * Decimal("500") / reference
    target_notional = desired * reference
    report["reference_price"] = format(reference, "f")
    report["target_notional_jpy"] = format(target_notional, "f")
    raw_delta = report["delta"]
    assert isinstance(raw_delta, dict)
    delta = {str(key): item for key, item in raw_delta.items()}
    delta["desired_size"] = format(desired, "f")
    delta["delta_size"] = format(desired, "f")
    report["delta"] = delta

    with pytest.raises(ValueError, match="成本 provenance 未绑定"):
        _validate_paper_fixture(chain, report)
    assert not _paper_receipt(chain).exists()


def test_unverifiable_spread_and_impact_contradiction_is_fail_closed(
    chain: FakeChain,
) -> None:
    report = _paper_filled_report()
    raw_cost = report["cost"]
    assert isinstance(raw_cost, dict)
    cost = {str(key): item for key, item in raw_cost.items()}
    cost["half_spread_bps"] = "999"
    cost["impact_bps"] = "999"
    report["cost"] = cost

    with pytest.raises(ValueError, match="成本 provenance 未绑定"):
        _validate_paper_fixture(chain, report)
    assert not _paper_receipt(chain).exists()


@pytest.mark.parametrize(
    ("section", "field"),
    [
        ("cost", "fee_bps"),
        ("cost", "half_spread_bps"),
        ("cost", "impact_bps"),
        ("cost", "slippage_vs_reference_bps"),
        ("cost", "total_cost_bps"),
        ("fee", "bps"),
        ("fill", "fee_jpy"),
    ],
)
def test_paper_rejects_negative_cost_components(
    chain: FakeChain, section: str, field: str,
) -> None:
    report = _paper_filled_report()
    raw_section = report[section]
    assert isinstance(raw_section, dict)
    changed = {str(key): item for key, item in raw_section.items()}
    changed[field] = "-1"
    report[section] = changed

    with pytest.raises(ValueError, match="不得为负"):
        _validate_paper_fixture(chain, report)
    assert not _paper_receipt(chain).exists()


def test_paper_rejects_favorable_buy_fill(chain: FakeChain) -> None:
    report = _paper_filled_report()
    raw_fill = report["fill"]
    assert isinstance(raw_fill, dict)
    fill = {str(key): item for key, item in raw_fill.items()}
    fill.update({
        "model_fill_price": "14999999",
        "notional_jpy": "149.99999",
        "fee_jpy": "0.074999995",
    })
    report["fill"] = fill

    with pytest.raises(ValueError, match="不是不利成交"):
        _validate_paper_fixture(chain, report)
    assert not _paper_receipt(chain).exists()


def test_paper_rejects_wrong_slippage_formula(chain: FakeChain) -> None:
    report = _paper_filled_report()
    raw_fill = report["fill"]
    assert isinstance(raw_fill, dict)
    fill = {str(key): item for key, item in raw_fill.items()}
    fill.update({
        "model_fill_price": "15000015",
        "notional_jpy": "150.00015",
        "fee_jpy": "0.075000075",
    })
    report["fill"] = fill

    with pytest.raises(ValueError, match="slippage 与成交公式不符"):
        _validate_paper_fixture(chain, report)
    assert not _paper_receipt(chain).exists()


def test_paper_rejects_wrong_total_cost_decomposition(chain: FakeChain) -> None:
    report = _paper_filled_report()
    raw_cost = report["cost"]
    assert isinstance(raw_cost, dict)
    cost = {str(key): item for key, item in raw_cost.items()}
    cost["total_cost_bps"] = "6"
    report["cost"] = cost

    with pytest.raises(ValueError, match="total_cost_bps 分解不符"):
        _validate_paper_fixture(chain, report)
    assert not _paper_receipt(chain).exists()


def test_post_commit_directory_cleanup_error_is_not_a_retry_signal(
    chain: FakeChain, monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_root = (chain.execution / shadow.SHADOW_ROOT / "locks").resolve()
    injected = False

    def cleanup_hook(phase: str, path: Path) -> None:
        nonlocal injected
        if (
            phase == "directory-handle-close-after-effect"
            and path == lock_root
            and _dry_receipt(chain).exists()
            and not injected
        ):
            injected = True
            raise OSError("simulated post-commit close failure")

    monkeypatch.setattr(shadow, "_path_race_hook", cleanup_hook)

    summary = _run_chain(chain, paper_enabled=False)

    assert injected
    assert summary["status"] == "completed"
    assert _dry_receipt(chain).is_file()


def test_cleanup_error_does_not_hide_precommit_executor_failure(
    chain: FakeChain, monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain.dry_returncode = 2
    lock_root = (chain.execution / shadow.SHADOW_ROOT / "locks").resolve()

    def cleanup_hook(phase: str, path: Path) -> None:
        if phase == "directory-handle-close-after-effect" and path == lock_root:
            raise OSError("simulated cleanup failure")

    monkeypatch.setattr(shadow, "_path_race_hook", cleanup_hook)

    with pytest.raises(RuntimeError, match=r"dry-run 失败\(2\)"):
        _run_chain(chain, paper_enabled=False)


def test_atomic_publish_cleanup_failure_after_link_stays_successful(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "artifact.json"
    original_unlink = Path.unlink

    def fail_temporary_unlink(
        path: Path, missing_ok: bool = False,
    ) -> None:
        if path.name.endswith(".tmp"):
            raise OSError("simulated temp cleanup failure")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_temporary_unlink)

    shadow._atomic_publish_new(
        target, b'{"durable":true}\n', allow_existing_identical=False,
    )

    assert target.read_bytes() == b'{"durable":true}\n'


def test_task_log_short_write_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def short_write(descriptor: int, body: bytes) -> int:
        del descriptor
        return max(0, len(body) - 1)

    monkeypatch.setattr(os, "write", short_write)

    with pytest.raises(OSError, match="短写"):
        shadow._append_record(tmp_path / "task.jsonl", {"status": "test"})


def test_post_receipt_task_log_failure_reuses_without_duplicate_execution(
    chain: FakeChain, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_log(path: Path, record: Mapping[str, object]) -> None:
        del path, record
        raise OSError("simulated durable append failure")

    monkeypatch.setattr(shadow, "_append_record", fail_log)

    first = _run_chain(chain)
    second = _run_chain(chain)

    assert first["status"] == "completed"
    assert first["task_log_status"] == "failed"
    assert second["status"] == "reused"
    assert second["task_log_status"] == "failed"
    assert _dry_receipt(chain).is_file()
    assert not _paper_receipt(chain).exists()
    assert len(chain.scripts("run_dry_run_executor.py")) == 1
    assert len(chain.scripts("run_paper_executor.py")) == 0


def test_isolated_python_startup_ignores_path_site_and_default_pyc(
    tmp_path: Path,
) -> None:
    trusted = tmp_path / "trusted"
    malicious = tmp_path / "malicious"
    trusted.mkdir()
    malicious.mkdir()
    victim = trusted / "victim.py"
    victim.write_text("VALUE = 'EVIL'\n", encoding="utf-8")
    original = victim.stat()
    compiled = py_compile.compile(str(victim), doraise=True)
    assert compiled is not None
    pyc_path = Path(compiled)
    victim.write_text("VALUE = 'SAFE'\n", encoding="utf-8")
    os.utime(
        victim,
        ns=(original.st_atime_ns, original.st_mtime_ns),
    )
    assert pyc_path.is_file()
    entry = trusted / "entry.py"
    entry.write_text("import victim; print(victim.VALUE)\n", encoding="utf-8")
    (malicious / "sitecustomize.py").write_text(
        "print('INJECTED-SITE')\n", encoding="utf-8",
    )
    (malicious / "victim.py").write_text(
        "VALUE = 'PATH-INJECTED'\n", encoding="utf-8",
    )
    sentinel = tmp_path / "pycache-disabled.sentinel"
    sentinel.write_bytes(shadow.PYCACHE_SENTINEL_BODY)
    child_env = dict(os.environ)
    child_env["PYTHONPATH"] = str(malicious)
    base = [
        sys.executable, "-I", "-S", "-B", "-X", "utf8",
        "-c", shadow._ISOLATED_RUNPY,
        "1", str(trusted), str(entry),
    ]
    control = subprocess.run(
        base,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=child_env,
    )
    safe = subprocess.run(
        [
            sys.executable, "-I", "-S", "-B", "-X",
            "utf8", "-X", f"pycache_prefix={sentinel}",
            "-c", shadow._ISOLATED_RUNPY,
            "1", str(trusted), str(entry),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=child_env,
    )

    assert control.returncode == 0
    assert control.stdout.strip() == "EVIL"
    assert safe.returncode == 0
    assert safe.stdout.strip() == "SAFE"
    assert "INJECTED" not in safe.stdout + safe.stderr


def test_isolated_python_startup_emits_utf8_under_non_utf8_locale(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    entry = source / "unicode_output.py"
    entry.write_text(
        "import sys\nprint('策略输出')\nsys.stderr.write('策略错误\\n')\n",
        encoding="utf-8",
    )
    sentinel = tmp_path / "pycache-disabled.sentinel"
    sentinel.write_bytes(shadow.PYCACHE_SENTINEL_BODY)
    child_env = dict(os.environ)
    child_env["PYTHONUTF8"] = "0"
    child_env["PYTHONIOENCODING"] = "cp932"

    result = subprocess.run(
        [
            sys.executable, "-I", "-S", "-B", "-X", "utf8", "-X",
            f"pycache_prefix={sentinel}", "-c", shadow._ISOLATED_RUNPY,
            "1", str(source), str(entry),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=child_env,
    )

    assert result.returncode == 0
    assert result.stdout == "策略输出\n"
    assert result.stderr == "策略错误\n"


@pytest.mark.skipif(shutil.which("git") is None, reason="需要 Git")
def test_runner_git_identity_ignores_inherited_git_redirection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """runner 自身须在污染的 GIT_* 环境下仍绑定真实 detached 仓。"""
    git_text = shutil.which("git")
    assert git_text is not None
    git = Path(git_text).resolve()
    repository = tmp_path / "detached-runtime"
    repository.mkdir()

    def invoke(*arguments: str) -> None:
        result = subprocess.run(
            [str(git), "-C", str(repository), *arguments],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        assert result.returncode == 0, result.stderr

    invoke("init", "--quiet")
    invoke("config", "core.autocrlf", "false")
    invoke("config", "user.email", "shadow-test@example.invalid")
    invoke("config", "user.name", "Shadow Test")
    (repository / "tracked.txt").write_text("tracked\n", encoding="utf-8")
    invoke("add", "tracked.txt")
    invoke("commit", "--quiet", "-m", "pinned")
    invoke("checkout", "--quiet", "--detach", "HEAD")
    monkeypatch.setenv("GIT_DIR", str(tmp_path / "bogus-git-dir"))
    monkeypatch.setenv("GIT_WORK_TREE", str(tmp_path / "bogus-work-tree"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(tmp_path / "bogus-index"))
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.fsmonitor")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "malicious-helper")
    git_identity = shadow._stable_identity(
        git, "test Git", allow_hardlinks=True,
    )

    with (
        shadow._use_git_executable(git_identity),
        shadow._guard_file_identities((git_identity,), "test Git"),
    ):
        identity = shadow._repository_identity(
            repository,
            "test repository",
            require_detached=True,
            reject_untracked_scopes=(".",),
        )

    assert identity.path == repository.resolve()
    assert len(identity.head_commit) == 40


@pytest.mark.skipif(os.name != "nt", reason="Windows Job Object contract")
def test_bounded_runner_timeout_kills_grandchild_process_tree(
    tmp_path: Path,
) -> None:
    child_code = (
        "import subprocess,sys,time;"
        "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']);"
        "print(p.pid,flush=True);time.sleep(30)"
    )

    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired) as captured:
        shadow._run(
            [sys.executable, "-I", "-S", "-X", "utf8", "-c", child_code],
            cwd=tmp_path,
            timeout_seconds=0.5,
        )
    elapsed = time.monotonic() - started

    output = str(captured.value.output).strip()
    assert output.isdecimal()
    assert "stdout_sha256=" in str(captured.value)
    grandchild = int(output)
    assert elapsed <= 0.5 + shadow.CHILD_CLEANUP_GRACE_SECONDS + 0.5
    deadline = time.monotonic() + 5
    while _windows_process_active(grandchild) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not _windows_process_active(grandchild)


def test_bounded_runner_rejects_output_over_memory_limit(
    tmp_path: Path,
) -> None:
    child_code = (
        "import sys,time;"
        f"sys.stdout.buffer.write(b'x'*{shadow.MAX_CHILD_OUTPUT_BYTES + 1});"
        "sys.stdout.buffer.flush();time.sleep(30)"
    )

    with pytest.raises(shadow.ChildOutputLimitError) as captured:
        shadow._run(
            [sys.executable, "-I", "-S", "-X", "utf8", "-c", child_code],
            cwd=tmp_path,
            timeout_seconds=10,
        )

    message = str(captured.value)
    assert f"stdout_bytes={shadow.MAX_CHILD_OUTPUT_BYTES + 1}" in message
    assert "stdout_sha256=" in message


def test_child_setup_time_counts_toward_execution_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_popen = subprocess.Popen

    def delayed_popen(*args: object, **kwargs: object) -> Any:
        time.sleep(0.2)
        return real_popen(*args, **kwargs)  # type: ignore[call-overload]

    monkeypatch.setattr(subprocess, "Popen", delayed_popen)
    started = time.monotonic()
    with pytest.raises(shadow.ChildTimeoutError):
        shadow._run(
            [sys.executable, "-I", "-S", "-X", "utf8", "-c", "pass"],
            cwd=tmp_path,
            timeout_seconds=0.05,
        )
    assert time.monotonic() - started < 1.0


def test_child_pipe_reader_error_fails_closed_and_kills_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    empty_sha = hashlib.sha256(b"").hexdigest()

    def failed_reader(
        pipe: BinaryIO,
        result: list[shadow._BoundedOutput],
        overflow: threading.Event,
        reader_failed: threading.Event,
    ) -> None:
        del overflow
        reader_failed.set()
        result.append(shadow._BoundedOutput(
            b"", 0, empty_sha, False, "OSError: synthetic reader failure",
        ))
        pipe.close()

    monkeypatch.setattr(shadow, "_read_bounded_pipe", failed_reader)
    started = time.monotonic()
    with pytest.raises(shadow.ChildOutputReadError, match="synthetic reader failure"):
        shadow._run(
            [
                sys.executable, "-I", "-S", "-X", "utf8", "-c",
                "import time;time.sleep(30)",
            ],
            cwd=tmp_path,
            timeout_seconds=5,
        )
    assert time.monotonic() - started < 1.0


def test_stalled_pipe_reader_is_bounded_by_cleanup_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    released = threading.Event()
    prefix = "frozen-shadow-pipe-"

    def stalled_reader(
        pipe: BinaryIO,
        result: list[shadow._BoundedOutput],
        overflow: threading.Event,
        reader_failed: threading.Event,
    ) -> None:
        del overflow
        while not pipe.closed:
            time.sleep(0.005)
        reader_failed.set()
        result.append(shadow._BoundedOutput(
            b"", 0, hashlib.sha256(b"").hexdigest(), False,
            "OSError: forced pipe close released reader",
        ))
        released.set()

    monkeypatch.setattr(shadow, "CHILD_CLEANUP_GRACE_SECONDS", 0.1)
    monkeypatch.setattr(shadow, "_read_bounded_pipe", stalled_reader)
    started = time.monotonic()
    with pytest.raises(
        shadow.ChildOutputReadError, match="forced pipe close released reader",
    ):
        shadow._run(
            [sys.executable, "-I", "-S", "-X", "utf8", "-c", "pass"],
            cwd=tmp_path,
            timeout_seconds=0.5,
        )
    assert time.monotonic() - started < 1.0
    assert released.is_set()
    assert not any(
        thread.name.startswith(prefix) for thread in threading.enumerate()
    )


def test_timeout_preserves_type_and_byte_identity_for_invalid_utf8(
    tmp_path: Path,
) -> None:
    body = b"\xff\xfe"
    code = (
        "import sys,time;"
        f"sys.stdout.buffer.write({body!r});"
        "sys.stdout.buffer.flush();time.sleep(30)"
    )

    with pytest.raises(shadow.ChildTimeoutError) as captured:
        shadow._run(
            [sys.executable, "-I", "-S", "-X", "utf8", "-c", code],
            cwd=tmp_path,
            timeout_seconds=0.2,
        )

    assert captured.value.stdout_identity.byte_count == len(body)
    assert captured.value.stdout_identity.sha256 == hashlib.sha256(body).hexdigest()
    assert "stdout_sha256=" in str(captured.value)


@pytest.mark.skipif(os.name != "nt", reason="Windows Job Object contract")
def test_windows_primary_failure_survives_closehandle_cleanup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_calls = 0
    real_close = shadow._close_windows_handle

    def primary_failure(*args: object, **kwargs: object) -> Any:
        del args, kwargs
        raise ValueError("bounded primary failure")

    def close_then_fail(handle: int) -> None:
        nonlocal close_calls
        close_calls += 1
        real_close(handle)
        raise OSError("synthetic CloseHandle report failure")

    monkeypatch.setattr(shadow, "_bounded_process_output", primary_failure)
    monkeypatch.setattr(shadow, "_close_windows_handle", close_then_fail)

    with pytest.raises(ValueError, match="bounded primary failure") as captured:
        shadow._run(
            [sys.executable, "-I", "-S", "-X", "utf8", "-c", "pass"],
            cwd=tmp_path,
            timeout_seconds=2,
        )

    assert close_calls == 1
    notes = "\n".join(getattr(captured.value, "__notes__", ()))
    assert "Job CloseHandle" in notes


@pytest.mark.skipif(os.name != "nt", reason="Windows Job Object contract")
def test_windows_assign_failure_preserves_immediate_last_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_kernel = shadow._windows_kernel32()

    class FailedAssign:
        argtypes: object = None
        restype: object = None

        def __call__(self, *args: object) -> int:
            del args
            ctypes.set_last_error(1234)
            return 0

    class KernelProxy:
        def __init__(self) -> None:
            self.AssignProcessToJobObject = FailedAssign()

        def __getattr__(self, name: str) -> Any:
            return getattr(real_kernel, name)

    proxy = KernelProxy()
    monkeypatch.setattr(shadow, "_windows_kernel32", lambda: proxy)

    with pytest.raises(OSError, match="无法把 child 纳入 Job Object") as captured:
        shadow._run(
            [sys.executable, "-I", "-S", "-X", "utf8", "-c", "pass"],
            cwd=tmp_path,
            timeout_seconds=2,
        )

    assert captured.value.args[0] == 1234


@pytest.mark.skipif(os.name != "nt", reason="Windows Job Object contract")
def test_windows_popen_baseexception_closes_job_once_and_preserves_primary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_close = shadow._close_windows_handle
    close_calls = 0

    def failed_popen(*args: object, **kwargs: object) -> Any:
        del args, kwargs
        raise KeyboardInterrupt("synthetic Popen failure")

    def counted_close(handle: int) -> None:
        nonlocal close_calls
        close_calls += 1
        real_close(handle)

    monkeypatch.setattr(subprocess, "Popen", failed_popen)
    monkeypatch.setattr(shadow, "_close_windows_handle", counted_close)

    with pytest.raises(KeyboardInterrupt, match="synthetic Popen failure"):
        shadow._run(
            [sys.executable, "-I", "-S", "-X", "utf8", "-c", "pass"],
            cwd=tmp_path,
            timeout_seconds=2,
        )

    assert close_calls == 1


@pytest.mark.skipif(os.name != "nt", reason="Windows Job Object contract")
def test_windows_resume_failure_is_primary_and_cleans_suspended_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_loader = getattr(ctypes, "WinDLL")

    class FailedResume:
        argtypes: object = None
        restype: object = None

        def __call__(self, *args: object) -> int:
            del args
            return -123

    class FakeNtdll:
        def __init__(self) -> None:
            self.NtResumeProcess = FailedResume()

    def loader(name: str, *args: object, **kwargs: object) -> Any:
        if name == "ntdll":
            return FakeNtdll()
        return real_loader(name, *args, **kwargs)

    monkeypatch.setattr(ctypes, "WinDLL", loader)

    with pytest.raises(OSError, match="无法恢复已纳入 Job 的 child") as captured:
        shadow._run(
            [sys.executable, "-I", "-S", "-X", "utf8", "-c", "pass"],
            cwd=tmp_path,
            timeout_seconds=2,
        )

    assert captured.value.args[0] == -123


def test_reader_thread_start_failure_is_primary_and_leaves_no_reader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_start = threading.Thread.start
    prefix = "frozen-shadow-pipe-"

    def fail_stderr_reader(thread: threading.Thread) -> None:
        if thread.name.startswith(prefix) and thread.name.endswith("stderr"):
            raise RuntimeError("synthetic reader thread start failure")
        real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_stderr_reader)

    with pytest.raises(
        RuntimeError, match="synthetic reader thread start failure",
    ):
        shadow._run(
            [
                sys.executable, "-I", "-S", "-X", "utf8", "-c",
                "import time;time.sleep(30)",
            ],
            cwd=tmp_path,
            timeout_seconds=2,
        )

    assert not any(
        thread.name.startswith(prefix) for thread in threading.enumerate()
    )


def test_kill_tree_failure_is_secondary_to_timeout(
    tmp_path: Path,
) -> None:
    process = subprocess.Popen(
        [
            sys.executable, "-I", "-S", "-X", "utf8", "-c",
            "import time;time.sleep(30)",
        ],
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    started = time.monotonic()

    def failed_tree_kill() -> None:
        raise OSError("synthetic tree kill failure")

    with pytest.raises(shadow.ChildTimeoutError) as captured:
        shadow._bounded_process_output(
            process,
            command=(sys.executable, "-c", "sleep"),
            timeout_seconds=0.1,
            execution_deadline=started + 0.1,
            hard_deadline=started + 0.5,
            kill_tree=failed_tree_kill,
        )

    notes = "\n".join(getattr(captured.value, "__notes__", ()))
    assert "process-tree termination" in notes
    assert "synthetic tree kill failure" in notes


def test_cli_requires_explicit_no_paper_before_runner_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def forbidden_run_shadow(*args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        nonlocal called
        called = True
        return {"status": "completed"}

    monkeypatch.setattr(shadow, "run_shadow", forbidden_run_shadow)
    arguments = [
        "--repository", str(tmp_path / "data"),
        "--runtime-root", str(tmp_path / "runtime"),
        "--execution-repository", str(tmp_path / "execution"),
        "--code-root", str(tmp_path / "code"),
        "--expected-code-head", "a" * 40,
        "--python-executable", str(tmp_path / "python.exe"),
        "--expected-python-sha256", "b" * 64,
        "--git-executable", str(tmp_path / "git.exe"),
        "--expected-git-sha256", "c" * 64,
        "--expected-execution-environment-tree-sha256", "d" * 64,
        "--plan-id", PLAN_ID,
    ]

    with pytest.raises(SystemExit) as captured:
        shadow.main(arguments)

    assert captured.value.code == 2
    assert called is False
