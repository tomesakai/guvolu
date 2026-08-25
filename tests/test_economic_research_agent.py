"""Research-only 经济代理的 PIT、台账与提案门禁测试。"""
from __future__ import annotations

import ctypes
import json
import os
import runpy
import shutil
import subprocess
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from typing import cast

import pytest

from guvolu.research import clock as research_clock
from guvolu.research import economic_agent
from guvolu.research.economic_agent import (
    EconomicAgentPolicy,
    EconomicObservation,
    append_economic_observations,
    build_economic_context,
    load_content_addressed_artifact,
    load_economic_observation_snapshot,
    load_economic_observations,
    load_economic_proposal_artifact,
    load_economic_run_receipt,
    parse_economic_policy,
    run_economic_research_agent,
    verify_economic_context,
    verify_economic_agent_ledger,
    write_content_addressed_artifact,
)
from guvolu.research.provenance import canonical_json, sha256_text, stable_identifier


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _economic_agent_main(arguments: list[str]) -> int:
    namespace = runpy.run_path("scripts/run_economic_research_agent.py")
    main = cast(Callable[[list[str]], int], namespace["main"])
    return main(arguments)


def _make_directory_alias(target: Path, link: Path) -> None:
    """Windows 建 junction，其余平台建目录 symlink。"""
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


def _policy(
    holdout: str | None = None,
    *,
    max_proposals: int = 2,
) -> EconomicAgentPolicy:
    series = {
        "growth_nowcast": {
            "dimension": "growth",
            "unit": "index",
            "neutral_value": 0.0,
            "scale": 1.0,
            "direction": "higher",
            "weight": 1.0,
            "max_age_seconds": 7 * 86400,
        },
        "inflation_nowcast": {
            "dimension": "inflation",
            "unit": "percent",
            "neutral_value": 2.0,
            "scale": 1.0,
            "direction": "higher",
            "weight": 1.0,
            "max_age_seconds": 7 * 86400,
        },
        "policy_rate": {
            "dimension": "rates",
            "unit": "percent",
            "neutral_value": 2.0,
            "scale": 1.0,
            "direction": "higher",
            "weight": 1.0,
            "max_age_seconds": 31 * 86400,
        },
        "liquidity": {
            "dimension": "liquidity",
            "unit": "index",
            "neutral_value": 0.0,
            "scale": 1.0,
            "direction": "higher",
            "weight": 1.0,
            "max_age_seconds": 7 * 86400,
        },
        "fx_risk": {
            "dimension": "fx",
            "unit": "index",
            "neutral_value": 0.0,
            "scale": 1.0,
            "direction": "higher",
            "weight": 1.0,
            "max_age_seconds": 7 * 86400,
        },
        "risk_appetite": {
            "dimension": "risk",
            "unit": "index",
            "neutral_value": 0.0,
            "scale": 1.0,
            "direction": "higher",
            "weight": 1.0,
            "max_age_seconds": 7 * 86400,
        },
    }
    return parse_economic_policy({
        "schema_version": 1,
        "series": series,
        "regime_threshold": 0.25,
        "proposal_gate": {
            "allowed_templates": {
                "trend": ["macro_regime_filter"],
                "price_breakout": ["macro_regime_filter"],
            },
            "template_parameters": {
                "macro_regime_filter": ["entry", "lookback"],
            },
            "max_proposals_per_run": max_proposals,
            "max_trial_budget_per_proposal": 12,
            "max_total_trial_budget": 16,
            "max_parameter_count": 2,
            "max_regime_count": 3,
            "max_horizon": 720,
            "holdout_start_time": holdout,
        },
    })


def _freeze_clock(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    timestamp = _time(value)
    monkeypatch.setattr(research_clock, "utc_now", lambda: timestamp)


def _observation(
    series_id: str,
    value: float,
    unit: str,
    *,
    event: str = "2026-05-01T00:00:00Z",
    available: str = "2026-05-02T00:00:00Z",
    ingest: str = "2026-05-02T00:01:00Z",
    supersedes: str | None = None,
    receipt: str = "a",
) -> dict[str, object]:
    return {
        "series_id": series_id,
        "value": value,
        "unit": unit,
        "event_time": event,
        "available_time": available,
        "ingest_time": ingest,
        "supersedes_revision_id": supersedes,
        "source_receipt": {
            "source_id": "official_release",
            "receipt_sha256": receipt * 64,
            "locator": f"receipts/{series_id}/{available}",
        },
    }


def _fresh_observations(path: Path) -> tuple[EconomicObservation, ...]:
    values = [
        _observation("growth_nowcast", 1.0, "index"),
        _observation("inflation_nowcast", 3.0, "percent", receipt="b"),
        _observation("policy_rate", 3.0, "percent", receipt="c"),
        _observation("liquidity", 1.0, "index", receipt="d"),
        _observation("fx_risk", 1.0, "index", receipt="e"),
        _observation("risk_appetite", 1.0, "index", receipt="f"),
    ]
    return append_economic_observations(path, values)


def _proposal(evidence_id: str, *, entry: float = 1.0) -> dict[str, object]:
    return {
        "hypothesis": "增长与风险偏好同步上行时，趋势策略可能更稳定。",
        "evidence_ids": [evidence_id],
        "family": "trend",
        "template": "macro_regime_filter",
        "parameter_bounds": {
            "entry": {"minimum": entry, "maximum": entry + 0.5, "step": 0.25},
            "lookback": {"minimum": 24, "maximum": 72, "step": 24},
        },
        "regimes": ["growth:strong", "risk:risk_on"],
        "horizon": {"unit": "hours", "minimum": 24, "maximum": 168},
        "falsification": "预登记 walk-forward OOS 成本后收益不显著或参数邻域不稳定则证伪。",
        "trial_budget": 8,
    }


def test_observation_ledger_is_atomic_pit_and_revision_chained(tmp_path: Path) -> None:
    """重复与修订分叉不落盘，ingest 不参与修订排序。"""
    ledger = tmp_path / "observations.jsonl"
    first = append_economic_observations(
        ledger, [_observation("growth_nowcast", 0.5, "index")],
    )[0]
    original = ledger.read_bytes()
    with pytest.raises(ValueError, match="重复 observation_id"):
        append_economic_observations(
            ledger, [_observation("growth_nowcast", 0.5, "index")],
        )
    assert ledger.read_bytes() == original
    with pytest.raises(ValueError, match="首版观测"):
        append_economic_observations(ledger, [_observation(
            "growth_nowcast",
            0.7,
            "index",
            event="2026-04-01T00:00:00Z",
            available="2026-05-04T00:00:00Z",
            ingest="2026-05-04T00:01:00Z",
            supersedes=first.revision_id,
            receipt="b",
        )])
    revision = append_economic_observations(ledger, [_observation(
        "growth_nowcast",
        1.0,
        "index",
        available="2026-05-04T00:00:00Z",
        ingest="2026-05-02T00:01:00Z",
        supersedes=first.revision_id,
        receipt="b",
    )])[0]
    loaded = load_economic_observations(ledger)
    assert [item.revision_id for item in loaded] == [first.revision_id, revision.revision_id]
    rows = ledger.read_text(encoding="utf-8").splitlines()
    assert all(canonical_json(json.loads(row)) == row for row in rows)
    assert json.loads(rows[1])["previous_record_sha256"] == json.loads(rows[0])[
        "record_sha256"
    ]


def test_observation_ledger_lock_prevents_concurrent_lost_update(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "observations.jsonl"
    barrier = Barrier(2)

    def append(value: dict[str, object]) -> None:
        barrier.wait()
        append_economic_observations(ledger, [value])

    values = (
        _observation("growth_nowcast", 0.5, "index"),
        _observation("inflation_nowcast", 3.0, "percent", receipt="b"),
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        tuple(executor.map(append, values))
    loaded = load_economic_observations(ledger)
    assert {item.series_id for item in loaded} == {
        "growth_nowcast",
        "inflation_nowcast",
    }


def test_first_ledger_ancestor_creation_rechecks_reparse_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = tmp_path / "new" / "nested" / "observations.jsonl"
    outside = tmp_path / "outside-ledger"
    outside.mkdir()
    moved = tmp_path / "created-parent-original"
    swapped: Path | None = None

    def swap(phase: str, candidate: Path) -> None:
        nonlocal swapped
        if phase != "directory-after-mkdir" or swapped is not None:
            return
        candidate.rename(moved)
        _make_directory_alias(outside, candidate)
        swapped = candidate

    monkeypatch.setattr(economic_agent, "_path_race_hook", swap)
    try:
        with pytest.raises(ValueError, match="reparse|junction|目录别名"):
            append_economic_observations(
                ledger,
                [_observation("growth_nowcast", 0.5, "index")],
            )
        assert list(outside.rglob("*")) == []
        assert not ledger.exists()
    finally:
        if swapped is not None:
            _remove_directory_alias(swapped)
            moved.rename(swapped)


def test_context_replay_excludes_future_revision_and_marks_missing_stale(
    tmp_path: Path,
) -> None:
    """decision_time 后才可知的修订不得改变历史语境。"""
    ledger = tmp_path / "observations.jsonl"
    first = append_economic_observations(ledger, [
        _observation("growth_nowcast", 0.5, "index"),
        _observation(
            "inflation_nowcast",
            4.0,
            "percent",
            event="2026-01-01T00:00:00Z",
            available="2026-01-02T00:00:00Z",
            ingest="2026-01-02T00:01:00Z",
            receipt="b",
        ),
    ])[0]
    before = build_economic_context(
        load_economic_observation_snapshot(ledger),
        _time("2026-05-03T00:00:00Z"),
        _policy(),
    )
    append_economic_observations(ledger, [_observation(
        "growth_nowcast",
        -1.0,
        "index",
        available="2026-05-04T00:00:00Z",
        ingest="2026-05-04T00:01:00Z",
        supersedes=first.revision_id,
        receipt="c",
    )])
    replay = build_economic_context(
        load_economic_observation_snapshot(ledger),
        _time("2026-05-03T00:00:00Z"),
        _policy(),
    )
    assert verify_economic_context(before, ledger, _policy()) == before["artifact_id"]
    assert replay["dimensions"] == before["dimensions"]
    assert replay["selected_observation_ids"] == before["selected_observation_ids"]
    assert replay["artifact_id"] != before["artifact_id"]
    dimensions = replay["dimensions"]
    assert isinstance(dimensions, dict)
    assert dimensions["growth"]["regime"] == "strong"
    assert dimensions["growth"]["data_status"] == "fresh"
    assert dimensions["inflation"]["data_status"] == "stale"
    assert dimensions["rates"]["data_status"] == "missing"
    selected_ids = cast(list[str], replay["selected_observation_ids"])
    quality = cast(dict[str, object], replay["quality"])
    assert first.observation_id in selected_ids
    assert quality["pit"] is True


def test_context_pit_uses_available_time_not_late_backfill_ingest_time(
    tmp_path: Path,
) -> None:
    """历史回补的下载时刻不得替代 available_time 承担防未来职责。"""
    ledger = tmp_path / "observations.jsonl"
    observation = append_economic_observations(ledger, [_observation(
        "growth_nowcast",
        1.0,
        "index",
        event="2026-01-01T00:00:00Z",
        available="2026-01-02T00:00:00Z",
        ingest="2026-05-10T00:00:00Z",
    )])[0]
    context = build_economic_context(
        load_economic_observation_snapshot(ledger),
        _time("2026-01-03T00:00:00Z"),
        _policy(),
    )
    selected_ids = cast(list[str], context["selected_observation_ids"])
    quality = cast(dict[str, object], context["quality"])
    assert observation.observation_id in selected_ids
    assert quality["pit_basis"] == (
        "available_time_lte_decision_time"
    )


def test_context_does_not_order_ingest_time_against_available_time(
    tmp_path: Path,
) -> None:
    """embargo 预载可早于合法可用时刻，PIT 仍只看 available。"""
    ledger = tmp_path / "observations.jsonl"
    observation = append_economic_observations(ledger, [_observation(
        "growth_nowcast",
        1.0,
        "index",
        ingest="2026-05-01T12:00:00Z",
        available="2026-05-02T00:00:00Z",
    )])[0]
    before = build_economic_context(
        load_economic_observation_snapshot(ledger),
        _time("2026-05-01T18:00:00Z"),
        _policy(),
    )
    after = build_economic_context(
        load_economic_observation_snapshot(ledger),
        _time("2026-05-03T00:00:00Z"),
        _policy(),
    )
    before_ids = cast(list[str], before["selected_observation_ids"])
    after_ids = cast(list[str], after["selected_observation_ids"])
    assert observation.observation_id not in before_ids
    assert observation.observation_id in after_ids


def test_context_artifact_is_canonical_content_addressed_and_tamper_evident(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "observations.jsonl"
    _fresh_observations(ledger)
    context = build_economic_context(
        load_economic_observation_snapshot(ledger),
        _time("2026-05-03T00:00:00Z"),
        _policy(),
    )
    path = write_content_addressed_artifact(
        tmp_path / "contexts", context, "economic-context",
    )
    assert load_content_addressed_artifact(path, "economic-context") == context
    tampered = json.loads(path.read_text(encoding="utf-8"))
    tampered["decision_time"] = "2026-05-03T01:00:00.000000+00:00"
    path.write_text(canonical_json(tampered) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="散列"):
        load_content_addressed_artifact(path, "economic-context")


def test_context_rebuild_binds_ledger_prefix_and_rejects_rehashed_forgery(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "observations.jsonl"
    _fresh_observations(ledger)
    policy = _policy()
    context = build_economic_context(
        load_economic_observation_snapshot(ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    context_id = str(context["artifact_id"])
    append_economic_observations(ledger, [_observation(
        "growth_nowcast",
        0.5,
        "index",
        event="2026-06-01T00:00:00Z",
        available="2026-06-02T00:00:00Z",
        ingest="2026-06-02T00:01:00Z",
        receipt="9",
    )])
    assert verify_economic_context(context, ledger, policy) == context_id

    forged = json.loads(canonical_json(context))
    forged["quality"]["pit"] = False
    del forged["artifact_id"]
    forged["artifact_id"] = stable_identifier("economic-context", forged)
    with pytest.raises(ValueError, match="不能由.*重建"):
        verify_economic_context(forged, ledger, policy)


def test_propose_rejects_context_with_retroactive_eligible_tail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation_ledger = tmp_path / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    policy = _policy()
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    append_economic_observations(observation_ledger, [_observation(
        "growth_nowcast",
        -1.0,
        "index",
        available="2026-05-02T12:00:00Z",
        ingest="2026-05-04T00:00:00Z",
        supersedes=observations[0].revision_id,
        receipt="9",
    )])
    rebuilt = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    dimensions = cast(dict[str, dict[str, object]], rebuilt["dimensions"])
    assert dimensions["growth"]["regime"] == "weak"
    assert verify_economic_context(context, observation_ledger, policy)
    _freeze_clock(monkeypatch, "2026-05-05T00:00:00Z")
    with pytest.raises(ValueError, match="前缀之后存在.*已可知"):
        run_economic_research_agent(
            context=context,
            proposals=[_proposal(observations[0].observation_id)],
            policy=policy,
            observation_ledger_path=observation_ledger,
            output=tmp_path / "output",
            ledger_path=tmp_path / "agent-runs.jsonl",
        )
    assert not (tmp_path / "agent-runs.jsonl").exists()


def test_agent_always_rejects_until_governance_binding_and_commits_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation_ledger = tmp_path / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    policy = _policy()
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    agent_ledger = tmp_path / "agent-runs.jsonl"
    output = tmp_path / "output"
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    result = run_economic_research_agent(
        context=context,
        proposals=[_proposal(observations[0].observation_id)],
        policy=policy,
        observation_ledger_path=observation_ledger,
        output=output,
        ledger_path=agent_ledger,
    )
    assert result.proposal_paths == ()
    assert result.accepted_proposal_ids == ()
    assert result.rejected_count == 1
    assert not (output / ".staging").exists()
    assert result.ledger_path == agent_ledger
    assert not (output / "runs").exists()
    receipt = load_economic_run_receipt(
        result.run_id,
        observation_ledger_path=observation_ledger,
        agent_ledger_path=agent_ledger,
        output=output,
        policy=policy,
    )
    attempts = cast(list[dict[str, object]], receipt["attempts"])
    assert attempts[0]["status"] == "rejected"
    assert attempts[0]["reasons"] == ["holdout_governance_unbound"]
    authority = cast(dict[str, object], receipt["authority"])
    assert authority["trade"] is False
    assert authority["promotion"] is False

    _freeze_clock(monkeypatch, "2026-05-03T00:02:00Z")
    second = run_economic_research_agent(
        context=context,
        proposals=[_proposal(observations[0].observation_id)],
        policy=policy,
        observation_ledger_path=observation_ledger,
        output=output,
        ledger_path=agent_ledger,
    )
    assert second.proposal_paths == () and second.rejected_count == 1
    rows = verify_economic_agent_ledger(
        agent_ledger,
        observation_ledger_path=observation_ledger,
        output=output,
        policy=policy,
    )
    assert len(rows) == 2


def test_agent_records_context_and_holdout_rejection_reasons(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation_ledger = tmp_path / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    policy = _policy(max_proposals=3)
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    invalid = _proposal("economic-observation-" + "0" * 64)
    invalid["regimes"] = ["growth:weak"]
    first = _proposal(observations[0].observation_id, entry=1.0)
    second = _proposal(observations[0].observation_id, entry=2.0)
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    result = run_economic_research_agent(
        context=context,
        proposals=[invalid, first, second],
        policy=policy,
        observation_ledger_path=observation_ledger,
        output=tmp_path / "output",
        ledger_path=tmp_path / "agent-runs.jsonl",
    )
    assert result.proposal_paths == ()
    assert result.rejected_count == 3
    rows = verify_economic_agent_ledger(
        tmp_path / "agent-runs.jsonl",
        observation_ledger_path=observation_ledger,
        output=tmp_path / "output",
        policy=policy,
    )
    receipt = cast(dict[str, object], rows[0]["receipt"])
    attempts = cast(list[dict[str, object]], receipt["attempts"])
    reasons = cast(list[str], attempts[0]["reasons"])
    assert "evidence_not_fresh_or_not_in_context" in reasons
    assert "regime_not_supported_by_current_context" in reasons
    assert "holdout_governance_unbound" in reasons
    assert all(
        "holdout_governance_unbound" in cast(list[str], item["reasons"])
        for item in attempts
    )

    holdout_policy = _policy(holdout="2026-05-03T00:00:30Z")
    holdout_context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        holdout_policy,
    )
    holdout = run_economic_research_agent(
        context=holdout_context,
        proposals=[_proposal(observations[0].observation_id, entry=3.0)],
        policy=holdout_policy,
        observation_ledger_path=observation_ledger,
        output=tmp_path / "holdout-output",
        ledger_path=tmp_path / "holdout-runs.jsonl",
    )
    assert holdout.proposal_paths == () and holdout.rejected_count == 1
    holdout_rows = verify_economic_agent_ledger(
        tmp_path / "holdout-runs.jsonl",
        observation_ledger_path=observation_ledger,
        output=tmp_path / "holdout-output",
        policy=holdout_policy,
    )
    holdout_receipt = cast(dict[str, object], holdout_rows[0]["receipt"])
    holdout_attempts = cast(list[dict[str, object]], holdout_receipt["attempts"])
    holdout_reasons = cast(list[str], holdout_attempts[0]["reasons"])
    assert "holdout_boundary_reached" in holdout_reasons
    assert "holdout_governance_unbound" in holdout_reasons


def test_contract_invalid_batch_fails_before_receipt_without_sealing_raw_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation_ledger = tmp_path / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    policy = _policy()
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    malformed = _proposal(observations[0].observation_id, entry=2.0)
    del malformed["hypothesis"]
    malformed["api_key"] = "must-not-be-persisted"
    output = tmp_path / "output"
    agent_ledger = tmp_path / "agent-runs.jsonl"
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    with pytest.raises(ValueError, match="合同非法.*不生成回执"):
        run_economic_research_agent(
            context=context,
            proposals=[_proposal(observations[0].observation_id), malformed],
            policy=policy,
            observation_ledger_path=observation_ledger,
            output=output,
            ledger_path=agent_ledger,
        )
    assert not output.exists()
    assert not agent_ledger.exists()
    assert not agent_ledger.with_name(agent_ledger.name + ".lock").exists()


def test_proposal_count_quota_fails_before_any_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation_ledger = tmp_path / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    policy = _policy(max_proposals=2)
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    output = tmp_path / "output"
    agent_ledger = tmp_path / "agent-runs.jsonl"
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    with pytest.raises(ValueError, match="max_proposals_per_run"):
        run_economic_research_agent(
            context=context,
            proposals=[
                _proposal(observations[0].observation_id, entry=float(index))
                for index in (1, 2, 3)
            ],
            policy=policy,
            observation_ledger_path=observation_ledger,
            output=output,
            ledger_path=agent_ledger,
        )
    assert not output.exists()
    assert not agent_ledger.exists()


def test_holdout_fails_closed_until_governance_registry_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation_ledger = tmp_path / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    policy = _policy(holdout="2026-06-01T00:00:00Z")
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    ledger = tmp_path / "agent-runs.jsonl"
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    before_boundary = run_economic_research_agent(
        context=context,
        proposals=[_proposal(observations[0].observation_id)],
        policy=policy,
        observation_ledger_path=observation_ledger,
        output=tmp_path / "output",
        ledger_path=ledger,
    )
    assert before_boundary.proposal_paths == ()
    receipt = load_economic_run_receipt(
        before_boundary.run_id,
        observation_ledger_path=observation_ledger,
        agent_ledger_path=ledger,
        output=tmp_path / "output",
        policy=policy,
    )
    receipt_authority = cast(dict[str, object], receipt["authority"])
    receipt_input = cast(dict[str, object], receipt["input_identity"])
    receipt_attempts = cast(list[dict[str, object]], receipt["attempts"])
    assert receipt_authority["holdout_governance_bound"] is False
    assert receipt_input["holdout_governance"] == {
        "binding": None,
        "bound": False,
        "reason": "holdout_governance_unbound",
    }
    assert receipt_attempts[0]["reasons"] == [
        "holdout_governance_unbound",
    ]

    _freeze_clock(monkeypatch, "2026-06-02T00:00:00Z")
    after_boundary = run_economic_research_agent(
        context=context,
        proposals=[_proposal(observations[0].observation_id, entry=2.0)],
        policy=policy,
        observation_ledger_path=observation_ledger,
        output=tmp_path / "output",
        ledger_path=ledger,
    )
    assert after_boundary.proposal_paths == ()
    rows = verify_economic_agent_ledger(
        ledger,
        observation_ledger_path=observation_ledger,
        output=tmp_path / "output",
        policy=policy,
    )
    second_receipt = cast(dict[str, object], rows[1]["receipt"])
    second_attempts = cast(list[dict[str, object]], second_receipt["attempts"])
    second_reasons = cast(list[str], second_attempts[0]["reasons"])
    assert "holdout_governance_unbound" in second_reasons
    assert "holdout_boundary_reached" in second_reasons


def test_ledger_hash_chain_detects_tampering(tmp_path: Path) -> None:
    ledger = tmp_path / "observations.jsonl"
    _fresh_observations(ledger)
    lines = ledger.read_text(encoding="utf-8").splitlines()
    row = json.loads(lines[2])
    row["value"] = 99.0
    lines[2] = canonical_json(row)
    ledger.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="散列"):
        load_economic_observations(ledger)


def test_semantic_verifier_rejects_fully_rehashed_run_forgery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation_ledger = tmp_path / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    policy = _policy()
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    output = tmp_path / "output"
    agent_ledger = tmp_path / "agent-runs.jsonl"
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    run_economic_research_agent(
        context=context,
        proposals=[_proposal(observations[0].observation_id)],
        policy=policy,
        observation_ledger_path=observation_ledger,
        output=output,
        ledger_path=agent_ledger,
    )
    row = cast(
        dict[str, object],
        json.loads(agent_ledger.read_text(encoding="utf-8")),
    )
    receipt = cast(
        dict[str, object],
        json.loads(canonical_json(row["receipt"])),
    )
    authority = cast(dict[str, object], receipt["authority"])
    authority["trade"] = True
    del receipt["artifact_id"]
    forged_run_id = stable_identifier("economic-agent-run", receipt)
    receipt["artifact_id"] = forged_run_id
    receipt_sha256 = sha256_text(canonical_json(receipt) + "\n")
    row["run_id"] = forged_run_id
    row["receipt_artifact_id"] = forged_run_id
    row["receipt_sha256"] = receipt_sha256
    row["receipt"] = receipt
    del row["record_sha256"]
    row["record_sha256"] = sha256_text(canonical_json(row))
    agent_ledger.write_text(canonical_json(row) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="不能由输入|authority"):
        verify_economic_agent_ledger(
            agent_ledger,
            observation_ledger_path=observation_ledger,
            output=output,
            policy=policy,
        )


def test_proposal_loader_requires_semantics_and_ledger_commitment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation_ledger = tmp_path / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    policy = _policy()
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    output = tmp_path / "output"
    agent_ledger = tmp_path / "agent-runs.jsonl"
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    run_economic_research_agent(
        context=context,
        proposals=[_proposal(observations[0].observation_id)],
        policy=policy,
        observation_ledger_path=observation_ledger,
        output=output,
        ledger_path=agent_ledger,
    )
    normalized = economic_agent._normalize_proposal(
        _proposal(observations[0].observation_id),
        policy.proposal_gate,
    )
    artifact = economic_agent._proposal_artifact(
        str(context["artifact_id"]),
        policy,
        normalized,
    )
    artifact_path = output / "proposals" / f"{artifact['artifact_id']}.json"
    with pytest.raises(ValueError, match="禁止写入 standalone"):
        write_content_addressed_artifact(
            output / "proposals",
            artifact,
            "economic-search-plan-proposal",
        )
    with pytest.raises(ValueError, match="专用语义"):
        load_content_addressed_artifact(
            artifact_path,
            "economic-search-plan-proposal",
        )
    with pytest.raises(ValueError, match="v1 没有 accepted proposal"):
        load_economic_proposal_artifact(
            artifact_path,
            context=context,
            policy=policy,
            observation_ledger_path=observation_ledger,
            agent_ledger_path=agent_ledger,
            output=output,
        )


def test_v1_embedded_receipts_remove_stage_and_landing_surface(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation_ledger = tmp_path / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    policy = _policy()
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    output = tmp_path / "output"
    agent_ledger = tmp_path / "agent-runs.jsonl"
    artifact_phases: list[str] = []

    def observe(phase: str, _path: Path) -> None:
        if phase.startswith(("stage-", "land-")):
            artifact_phases.append(phase)

    monkeypatch.setattr(economic_agent, "_path_race_hook", observe)
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    result = run_economic_research_agent(
        context=context,
        proposals=[_proposal(observations[0].observation_id)],
        policy=policy,
        observation_ledger_path=observation_ledger,
        output=output,
        ledger_path=agent_ledger,
    )
    rows = verify_economic_agent_ledger(
        agent_ledger,
        observation_ledger_path=observation_ledger,
        output=output,
        policy=policy,
    )
    assert artifact_phases == []
    assert not hasattr(economic_agent, "_stage_artifacts")
    assert not hasattr(economic_agent, "_land_staged_artifact")
    assert not output.exists()
    assert result.ledger_path == agent_ledger
    assert rows[0]["receipt_storage"] == "embedded_in_ledger"
    assert rows[0]["artifact_commitments"] == []


@pytest.mark.parametrize("ledger_kind", ["observation", "agent_run"])
def test_ledger_double_parent_swap_rolls_back_without_external_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ledger_kind: str,
) -> None:
    ledger_parent = tmp_path / f"{ledger_kind}-parent"
    ledger_parent.mkdir()
    outside = tmp_path / f"{ledger_kind}-outside"
    outside.mkdir()
    moved = tmp_path / f"{ledger_kind}-original"
    swapped_parent: Path | None = None

    def swap(phase: str, parent: Path) -> None:
        nonlocal swapped_parent
        if parent != ledger_parent:
            return
        if phase == "ledger-after-final-check" and swapped_parent is None:
            parent.rename(moved)
            _make_directory_alias(outside, parent)
            swapped_parent = parent
        elif phase == "ledger-after-install" and swapped_parent is not None:
            _remove_directory_alias(parent)
            moved.rename(parent)
            swapped_parent = None

    try:
        if ledger_kind == "observation":
            committed = ledger_parent / "observations.jsonl"
            append_economic_observations(
                committed,
                [_observation("growth_nowcast", 0.5, "index")],
            )
            before = committed.read_bytes()
            monkeypatch.setattr(economic_agent, "_path_race_hook", swap)
            with pytest.raises((OSError, ValueError)):
                append_economic_observations(
                    committed,
                    [_observation("inflation_nowcast", 3.0, "percent")],
                )
        else:
            observation_ledger = tmp_path / "observations.jsonl"
            observations = _fresh_observations(observation_ledger)
            policy = _policy()
            context = build_economic_context(
                load_economic_observation_snapshot(observation_ledger),
                _time("2026-05-03T00:00:00Z"),
                policy,
            )
            _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
            committed = ledger_parent / "agent-runs.jsonl"
            run_economic_research_agent(
                context=context,
                proposals=[_proposal(observations[0].observation_id)],
                policy=policy,
                observation_ledger_path=observation_ledger,
                output=tmp_path / "output",
                ledger_path=committed,
            )
            before = committed.read_bytes()
            monkeypatch.setattr(economic_agent, "_path_race_hook", swap)
            _freeze_clock(monkeypatch, "2026-05-03T00:02:00Z")
            with pytest.raises((OSError, ValueError)):
                run_economic_research_agent(
                    context=context,
                    proposals=[_proposal(
                        observations[0].observation_id,
                        entry=2.0,
                    )],
                    policy=policy,
                    observation_ledger_path=observation_ledger,
                    output=tmp_path / "output",
                    ledger_path=committed,
                )
        assert committed.read_bytes() == before
        assert list(outside.rglob("*")) == []
    finally:
        if swapped_parent is not None:
            _remove_directory_alias(swapped_parent)
            moved.rename(swapped_parent)


@pytest.mark.parametrize("ledger_kind", ["observation", "agent_run"])
def test_ledger_hardlink_during_inode_append_restores_old_commitment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ledger_kind: str,
) -> None:
    ledger_parent = tmp_path / f"{ledger_kind}-hardlink-parent"
    ledger_parent.mkdir()
    committed = ledger_parent / f"{ledger_kind}.jsonl"
    external = tmp_path / f"{ledger_kind}-external-link.jsonl"
    link_error: OSError | None = None

    if ledger_kind == "observation":
        append_economic_observations(
            committed,
            [_observation("growth_nowcast", 0.5, "index")],
        )
        before = committed.read_bytes()
    else:
        observation_ledger = tmp_path / "observations.jsonl"
        observations = _fresh_observations(observation_ledger)
        policy = _policy()
        context = build_economic_context(
            load_economic_observation_snapshot(observation_ledger),
            _time("2026-05-03T00:00:00Z"),
            policy,
        )
        _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
        run_economic_research_agent(
            context=context,
            proposals=[_proposal(observations[0].observation_id)],
            policy=policy,
            observation_ledger_path=observation_ledger,
            output=tmp_path / "output",
            ledger_path=committed,
        )
        before = committed.read_bytes()

    def hardlink_after_write(phase: str, parent: Path) -> None:
        nonlocal link_error
        if phase != "ledger-after-write" or parent != ledger_parent:
            return
        try:
            os.link(committed, external)
        except OSError as error:
            link_error = error
            raise

    monkeypatch.setattr(
        economic_agent,
        "_path_race_hook",
        hardlink_after_write,
    )
    try:
        with pytest.raises((OSError, ValueError)):
            if ledger_kind == "observation":
                append_economic_observations(
                    committed,
                    [_observation("inflation_nowcast", 3.0, "percent")],
                )
            else:
                _freeze_clock(monkeypatch, "2026-05-03T00:02:00Z")
                run_economic_research_agent(
                    context=context,
                    proposals=[_proposal(
                        observations[0].observation_id,
                        entry=2.0,
                    )],
                    policy=policy,
                    observation_ledger_path=observation_ledger,
                    output=tmp_path / "output",
                    ledger_path=committed,
                )
        assert committed.read_bytes() == before
        if external.exists():
            assert external.read_bytes() == before
        else:
            assert link_error is not None
        assert list(ledger_parent.glob(f".{committed.name}.append-*")) == []
    finally:
        if external.exists():
            external.unlink()


@pytest.mark.parametrize("existing", [False, True])
def test_ledger_interrupted_short_write_restores_original_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    existing: bool,
) -> None:
    ledger = tmp_path / "short-write-observations.jsonl"
    if existing:
        append_economic_observations(
            ledger,
            [_observation("growth_nowcast", 0.5, "index")],
        )
        before: bytes | None = ledger.read_bytes()
        value = _observation("inflation_nowcast", 3.0, "percent")
    else:
        before = None
        value = _observation("growth_nowcast", 0.5, "index")
    original_write_all = economic_agent._write_descriptor_all
    raw_write = os.write
    interrupted = False

    def interrupt_once(descriptor: int, body: bytes) -> None:
        nonlocal interrupted
        if interrupted:
            original_write_all(descriptor, body)
            return
        interrupted = True
        raw_write(descriptor, body[:max(1, len(body) // 2)])
        raise OSError("injected short write")

    monkeypatch.setattr(
        economic_agent,
        "_write_descriptor_all",
        interrupt_once,
    )
    with pytest.raises(OSError, match="injected short write"):
        append_economic_observations(ledger, [value])
    if before is None:
        assert not ledger.exists()
    else:
        assert ledger.read_bytes() == before


@pytest.mark.parametrize("ledger_kind", ["observation", "agent_run"])
@pytest.mark.parametrize("existing", [False, True])
def test_close_after_effect_is_post_commit_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ledger_kind: str,
    existing: bool,
) -> None:
    ledger_parent = tmp_path / f"{ledger_kind}-close-parent"
    ledger_parent.mkdir()
    committed = ledger_parent / f"{ledger_kind}.jsonl"
    external = tmp_path / f"{ledger_kind}-close-external.jsonl"
    before = b""

    if ledger_kind == "observation":
        if existing:
            append_economic_observations(
                committed,
                [_observation("growth_nowcast", 0.5, "index")],
            )
            before = committed.read_bytes()
        value = _observation(
            "inflation_nowcast" if existing else "growth_nowcast",
            3.0 if existing else 0.5,
            "percent" if existing else "index",
        )
    else:
        observation_ledger = tmp_path / "observations.jsonl"
        observations = _fresh_observations(observation_ledger)
        policy = _policy()
        context = build_economic_context(
            load_economic_observation_snapshot(observation_ledger),
            _time("2026-05-03T00:00:00Z"),
            policy,
        )
        if existing:
            _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
            run_economic_research_agent(
                context=context,
                proposals=[_proposal(observations[0].observation_id)],
                policy=policy,
                observation_ledger_path=observation_ledger,
                output=tmp_path / "output",
                ledger_path=committed,
            )
            before = committed.read_bytes()

    close_target_ready = False
    close_error_injected = False
    real_close = os.close

    def mark_commit_boundary(phase: str, parent: Path) -> None:
        nonlocal close_target_ready
        if phase == "ledger-before-commit" and parent == ledger_parent:
            close_target_ready = True

    def close_then_raise(descriptor: int) -> None:
        nonlocal close_target_ready, close_error_injected
        if not close_target_ready or close_error_injected:
            real_close(descriptor)
            return
        close_target_ready = False
        close_error_injected = True
        real_close(descriptor)
        os.link(committed, external)
        raise OSError("injected close-after-effect")

    monkeypatch.setattr(
        economic_agent,
        "_path_race_hook",
        mark_commit_boundary,
    )
    monkeypatch.setattr(
        "guvolu.research.economic_agent.os.close", close_then_raise,
    )
    try:
        if ledger_kind == "observation":
            append_economic_observations(committed, [value])
        else:
            _freeze_clock(
                monkeypatch,
                "2026-05-03T00:02:00Z" if existing
                else "2026-05-03T00:01:00Z",
            )
            result = run_economic_research_agent(
                context=context,
                proposals=[_proposal(
                    observations[0].observation_id,
                    entry=2.0 if existing else 1.0,
                )],
                policy=policy,
                observation_ledger_path=observation_ledger,
                output=tmp_path / "output",
                ledger_path=committed,
            )
        assert close_error_injected is True
        assert external.read_bytes() == committed.read_bytes()
        assert len(committed.read_bytes()) > len(before)
        assert committed.read_bytes().startswith(before)
        external.unlink()
        if ledger_kind == "observation":
            assert len(load_economic_observations(committed)) == 1 + int(existing)
        else:
            rows = verify_economic_agent_ledger(
                committed,
                observation_ledger_path=observation_ledger,
                output=tmp_path / "output",
                policy=policy,
            )
            assert len(rows) == 1 + int(existing)
            assert rows[-1]["run_id"] == result.run_id
    finally:
        if external.exists():
            external.unlink()


@pytest.mark.parametrize("ledger_kind", ["observation", "agent_run"])
@pytest.mark.parametrize(
    "cleanup_phase",
    [
        "ledger-lock-release-after-effect",
        "ledger-lock-close-after-effect",
        "ledger-parent-close-after-effect",
    ],
)
def test_outer_cleanup_after_effect_is_post_commit_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ledger_kind: str,
    cleanup_phase: str,
) -> None:
    """Durable ledger commits must not be reported as retryable cleanup failures."""
    ledger_parent = tmp_path / f"{ledger_kind}-outer-cleanup"
    ledger_parent.mkdir()
    ledger = ledger_parent / f"{ledger_kind}.jsonl"
    cleanup_seen = False

    if ledger_kind == "agent_run":
        observation_ledger = tmp_path / "observations.jsonl"
        observations = _fresh_observations(observation_ledger)
        policy = _policy()
        context = build_economic_context(
            load_economic_observation_snapshot(observation_ledger),
            _time("2026-05-03T00:00:00Z"),
            policy,
        )

    def fail_after_cleanup(phase: str, parent: Path) -> None:
        nonlocal cleanup_seen
        if (
            phase == cleanup_phase
            and parent == ledger_parent
            and not cleanup_seen
        ):
            cleanup_seen = True
            raise OSError("injected outer cleanup-after-effect")

    monkeypatch.setattr(economic_agent, "_path_race_hook", fail_after_cleanup)
    if ledger_kind == "observation":
        append_economic_observations(
            ledger,
            [_observation("growth_nowcast", 0.5, "index")],
        )
        assert len(load_economic_observations(ledger)) == 1
    else:
        _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
        result = run_economic_research_agent(
            context=context,
            proposals=[_proposal(observations[0].observation_id)],
            policy=policy,
            observation_ledger_path=observation_ledger,
            output=tmp_path / "output",
            ledger_path=ledger,
        )
        rows = verify_economic_agent_ledger(
            ledger,
            observation_ledger_path=observation_ledger,
            output=tmp_path / "output",
            policy=policy,
        )
        assert len(rows) == 1
        assert rows[0]["run_id"] == result.run_id
    assert cleanup_seen is True


def test_outer_cleanup_error_does_not_hide_body_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger_parent = tmp_path / "body-failure-cleanup"
    ledger_parent.mkdir()
    ledger = ledger_parent / "observations.jsonl"
    cleanup_seen = False

    def fail_body_and_cleanup(phase: str, parent: Path) -> None:
        nonlocal cleanup_seen
        if parent != ledger_parent:
            return
        if phase == "ledger-after-write":
            raise ValueError("injected primary body failure")
        if phase == "ledger-parent-close-after-effect":
            cleanup_seen = True
            raise OSError("injected secondary cleanup failure")

    monkeypatch.setattr(economic_agent, "_path_race_hook", fail_body_and_cleanup)
    with pytest.raises(ValueError, match="primary body failure"):
        append_economic_observations(
            ledger,
            [_observation("growth_nowcast", 0.5, "index")],
        )
    assert cleanup_seen is True
    assert not ledger.exists()


@pytest.mark.parametrize("existing", [False, True])
def test_rollback_data_close_after_effect_preserves_primary_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    existing: bool,
) -> None:
    ledger_parent = tmp_path / "rollback-data-close"
    ledger_parent.mkdir()
    ledger = ledger_parent / "observations.jsonl"
    before: bytes | None = None
    if existing:
        append_economic_observations(
            ledger,
            [_observation("growth_nowcast", 0.5, "index")],
        )
        before = ledger.read_bytes()
    close_target_ready = False
    close_error_injected = False
    real_close = os.close

    def fail_after_write(phase: str, parent: Path) -> None:
        nonlocal close_target_ready
        if phase == "ledger-after-write" and parent == ledger_parent:
            close_target_ready = True
            raise ValueError("injected primary append failure")

    def close_then_raise(descriptor: int) -> None:
        nonlocal close_target_ready, close_error_injected
        if not close_target_ready or close_error_injected:
            real_close(descriptor)
            return
        close_target_ready = False
        close_error_injected = True
        real_close(descriptor)
        raise OSError("injected rollback close-after-effect")

    monkeypatch.setattr(economic_agent, "_path_race_hook", fail_after_write)
    monkeypatch.setattr(
        "guvolu.research.economic_agent.os.close",
        close_then_raise,
    )
    value = _observation(
        "inflation_nowcast" if existing else "growth_nowcast",
        3.0 if existing else 0.5,
        "percent" if existing else "index",
    )
    with pytest.raises(ValueError, match="primary append failure"):
        append_economic_observations(ledger, [value])
    assert close_error_injected is True
    if before is None:
        assert not ledger.exists()
    else:
        assert ledger.read_bytes() == before


def test_update_open_close_after_effect_preserves_primary_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = tmp_path / "update-open-close.jsonl"
    original_lstat = economic_agent._pinned_lstat
    lstat_calls = 0
    close_ready = False
    close_injected = False
    real_close = os.close

    def fail_after_open(
        locked: economic_agent._LockedLedger,
    ) -> os.stat_result | None:
        nonlocal lstat_calls, close_ready
        lstat_calls += 1
        if lstat_calls == 3:
            close_ready = True
            raise ValueError("injected update-open primary failure")
        return original_lstat(locked)

    def close_then_raise(descriptor: int) -> None:
        nonlocal close_ready, close_injected
        if not close_ready or close_injected:
            real_close(descriptor)
            return
        close_ready = False
        close_injected = True
        real_close(descriptor)
        raise OSError("injected update-open close-after-effect")

    monkeypatch.setattr(economic_agent, "_pinned_lstat", fail_after_open)
    monkeypatch.setattr(
        "guvolu.research.economic_agent.os.close",
        close_then_raise,
    )
    with pytest.raises(ValueError, match="update-open primary failure"):
        append_economic_observations(
            ledger,
            [_observation("growth_nowcast", 0.5, "index")],
        )
    assert close_injected is True
    assert not ledger.exists()


def test_pre_pending_close_after_effect_preserves_primary_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = tmp_path / "pre-pending-close.jsonl"
    close_ready = False
    close_injected = False
    real_close = os.close

    def fail_before_pending(descriptor: int) -> bytes:
        nonlocal close_ready
        close_ready = True
        raise ValueError("injected pre-pending primary failure")

    def close_then_raise(descriptor: int) -> None:
        nonlocal close_ready, close_injected
        if not close_ready or close_injected:
            real_close(descriptor)
            return
        close_ready = False
        close_injected = True
        real_close(descriptor)
        raise OSError("injected pre-pending close-after-effect")

    monkeypatch.setattr(
        economic_agent,
        "_read_descriptor_bytes",
        fail_before_pending,
    )
    monkeypatch.setattr(
        "guvolu.research.economic_agent.os.close",
        close_then_raise,
    )
    with pytest.raises(ValueError, match="pre-pending primary failure"):
        append_economic_observations(
            ledger,
            [_observation("growth_nowcast", 0.5, "index")],
        )
    assert close_injected is True
    assert not ledger.exists()


def test_read_only_load_does_not_leave_new_files(tmp_path: Path) -> None:
    ledger_parent = tmp_path / "read-only-no-artifacts"
    ledger_parent.mkdir()
    ledger = ledger_parent / "observations.jsonl"
    _fresh_observations(ledger)
    before = {
        path.name: path.read_bytes()
        for path in ledger_parent.iterdir()
        if path.is_file()
    }
    load_economic_observation_snapshot(ledger)
    after = {
        path.name: path.read_bytes()
        for path in ledger_parent.iterdir()
        if path.is_file()
    }
    assert after == before


@pytest.mark.skipif(os.name == "nt", reason="POSIX persistent lock file")
def test_posix_read_only_load_does_not_recreate_missing_lock(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "observations.jsonl"
    _fresh_observations(ledger)
    lock_path = ledger.with_name(ledger.name + ".lock")
    lock_path.unlink()
    with pytest.raises(FileNotFoundError):
        load_economic_observation_snapshot(ledger)
    assert not lock_path.exists()


@pytest.mark.parametrize(
    "cleanup_phase",
    [
        "ledger-lock-release-after-effect",
        "ledger-lock-close-after-effect",
        "ledger-parent-close-after-effect",
    ],
)
def test_artifact_outer_cleanup_after_commit_is_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cleanup_phase: str,
) -> None:
    ledger = tmp_path / "artifact-observations.jsonl"
    _fresh_observations(ledger)
    context = build_economic_context(
        load_economic_observation_snapshot(ledger),
        _time("2026-05-03T00:00:00Z"),
        _policy(),
    )
    output = tmp_path / "artifact-output"
    cleanup_seen = False

    def fail_after_cleanup(phase: str, parent: Path) -> None:
        nonlocal cleanup_seen
        if (
            phase == cleanup_phase
            and parent == output
            and not cleanup_seen
        ):
            cleanup_seen = True
            raise OSError("injected artifact cleanup-after-effect")

    monkeypatch.setattr(economic_agent, "_path_race_hook", fail_after_cleanup)
    path = write_content_addressed_artifact(
        output,
        context,
        "economic-context",
    )
    monkeypatch.setattr(economic_agent, "_path_race_hook", lambda *_: None)
    assert cleanup_seen is True
    assert load_content_addressed_artifact(path, "economic-context") == context


@pytest.mark.parametrize(
    "cleanup_phase",
    [
        *(
            ["ledger-anchor-close-after-effect"]
            if os.name == "nt"
            else []
        ),
        "ledger-lock-release-after-effect",
        "ledger-lock-close-after-effect",
        "ledger-parent-close-after-effect",
    ],
)
def test_run_outer_observation_cleanup_after_commit_is_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cleanup_phase: str,
) -> None:
    """The inner durable run commit governs every enclosing lock cleanup."""
    observation_parent = tmp_path / "outer-observation-cleanup"
    observation_parent.mkdir()
    observation_ledger = observation_parent / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    policy = _policy()
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    agent_ledger = tmp_path / "agent-runs.jsonl"
    cleanup_seen = False

    def fail_outer_cleanup_after_effect(phase: str, parent: Path) -> None:
        nonlocal cleanup_seen
        if (
            phase == cleanup_phase
            and parent == observation_parent
            and not cleanup_seen
        ):
            cleanup_seen = True
            raise OSError("injected outer observation cleanup-after-effect")

    monkeypatch.setattr(
        economic_agent,
        "_path_race_hook",
        fail_outer_cleanup_after_effect,
    )
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    result = run_economic_research_agent(
        context=context,
        proposals=[_proposal(observations[0].observation_id)],
        policy=policy,
        observation_ledger_path=observation_ledger,
        output=tmp_path / "output",
        ledger_path=agent_ledger,
    )
    monkeypatch.setattr(economic_agent, "_path_race_hook", lambda *_: None)
    rows = verify_economic_agent_ledger(
        agent_ledger,
        observation_ledger_path=observation_ledger,
        output=tmp_path / "output",
        policy=policy,
    )
    assert cleanup_seen is True
    assert len(rows) == 1
    assert rows[0]["run_id"] == result.run_id


def test_run_outer_observation_postcommit_identity_change_is_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A committed inner run cannot be reversed by outer read-only checks."""
    observation_parent = tmp_path / "outer-observation-postcommit"
    observation_parent.mkdir()
    observation_ledger = observation_parent / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    agent_parent = tmp_path / "inner-agent-postcommit"
    agent_parent.mkdir()
    agent_ledger = agent_parent / "agent-runs.jsonl"
    observation_alias = tmp_path / "observation-postcommit-hardlink.jsonl"
    policy = _policy()
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    injected = False

    def hardlink_after_inner_commit(phase: str, parent: Path) -> None:
        nonlocal injected
        if (
            phase == "ledger-lock-release-after-effect"
            and parent == agent_parent
            and not injected
        ):
            os.link(observation_ledger, observation_alias)
            injected = True

    monkeypatch.setattr(
        economic_agent,
        "_path_race_hook",
        hardlink_after_inner_commit,
    )
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    try:
        result = run_economic_research_agent(
            context=context,
            proposals=[_proposal(observations[0].observation_id)],
            policy=policy,
            observation_ledger_path=observation_ledger,
            output=tmp_path / "output",
            ledger_path=agent_ledger,
        )
        assert injected is True
    finally:
        observation_alias.unlink(missing_ok=True)
    monkeypatch.setattr(economic_agent, "_path_race_hook", lambda *_: None)
    rows = verify_economic_agent_ledger(
        agent_ledger,
        observation_ledger_path=observation_ledger,
        output=tmp_path / "output",
        policy=policy,
    )
    assert len(rows) == 1
    assert rows[0]["run_id"] == result.run_id


def test_run_rejects_observation_identity_change_before_inner_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The observation dependency is revalidated at the agent commit point."""
    observation_parent = tmp_path / "outer-observation-precommit"
    observation_parent.mkdir()
    observation_ledger = observation_parent / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    agent_parent = tmp_path / "inner-agent-precommit"
    agent_parent.mkdir()
    agent_ledger = agent_parent / "agent-runs.jsonl"
    observation_alias = tmp_path / "observation-precommit-hardlink.jsonl"
    policy = _policy()
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    injected = False

    def hardlink_before_inner_commit(phase: str, parent: Path) -> None:
        nonlocal injected
        if (
            phase == "ledger-before-commit"
            and parent == agent_parent
            and not injected
        ):
            os.link(observation_ledger, observation_alias)
            injected = True

    monkeypatch.setattr(
        economic_agent,
        "_path_race_hook",
        hardlink_before_inner_commit,
    )
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    try:
        with pytest.raises(ValueError, match="单链接"):
            run_economic_research_agent(
                context=context,
                proposals=[_proposal(observations[0].observation_id)],
                policy=policy,
                observation_ledger_path=observation_ledger,
                output=tmp_path / "output",
                ledger_path=agent_ledger,
            )
        assert injected is True
        assert not agent_ledger.exists()
    finally:
        observation_alias.unlink(missing_ok=True)


def test_windows_close_handle_zero_is_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_calls = 0

    def close_handle(_handle: object) -> int:
        nonlocal close_calls
        close_calls += 1
        return 0

    kernel = SimpleNamespace(CloseHandle=close_handle)
    monkeypatch.setattr(economic_agent, "_windows_kernel32", lambda: kernel)
    monkeypatch.setattr(
        ctypes,
        "get_last_error",
        lambda: 6,
        raising=False,
    )
    with pytest.raises(OSError, match="句柄关闭失败"):
        economic_agent._windows_close_handle(42)
    assert close_calls == 1


def test_windows_open_failure_preserves_primary_close_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_calls = 0

    def create_file(*_arguments: object) -> int:
        return 42

    def get_information(*_arguments: object) -> int:
        return 0

    def close_handle(_handle: object) -> int:
        nonlocal close_calls
        close_calls += 1
        return 0

    kernel = SimpleNamespace(
        CreateFileW=create_file,
        GetFileInformationByHandle=get_information,
        CloseHandle=close_handle,
    )
    monkeypatch.setattr(economic_agent, "_windows_kernel32", lambda: kernel)
    monkeypatch.setattr(
        ctypes,
        "get_last_error",
        lambda: 87,
        raising=False,
    )
    with pytest.raises(OSError, match="无法读取测试文件身份"):
        economic_agent._windows_open_regular_file(
            tmp_path / "missing.jsonl",
            "测试文件",
        )
    assert close_calls == 1


def test_windows_mutex_release_zero_is_error_and_closes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_calls = 0
    close_calls = 0

    def create_mutex(*_arguments: object) -> int:
        return 42

    def wait(*_arguments: object) -> int:
        return 0

    def release(_handle: object) -> int:
        nonlocal release_calls
        release_calls += 1
        return 0

    def close_handle(_handle: object) -> int:
        nonlocal close_calls
        close_calls += 1
        return 1

    kernel = SimpleNamespace(
        CreateMutexW=create_mutex,
        WaitForSingleObject=wait,
        ReleaseMutex=release,
        CloseHandle=close_handle,
    )
    monkeypatch.setattr(economic_agent, "_windows_kernel32", lambda: kernel)
    monkeypatch.setattr(
        ctypes,
        "get_last_error",
        lambda: 288,
        raising=False,
    )
    with pytest.raises(OSError, match="mutex 释放失败"):
        with economic_agent._windows_named_ledger_mutex(
            tmp_path / "mutex.jsonl",
        ):
            pass
    assert release_calls == 1
    assert close_calls == 1


@pytest.mark.skipif(os.name != "nt", reason="Windows parent-handle cleanup")
def test_read_only_windows_parent_cleanup_closes_every_handle_after_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger_parent = tmp_path / "read-only-parent-cleanup"
    ledger_parent.mkdir()
    ledger = ledger_parent / "observations.jsonl"
    _fresh_observations(ledger)
    expected_closes = len(
        economic_agent._capture_directory_chain(
            ledger_parent,
            "test ledger parent",
        ),
    )
    close_hooks = 0

    def fail_first_parent_close(phase: str, parent: Path) -> None:
        nonlocal close_hooks
        if phase != "ledger-parent-close-after-effect" or parent != ledger_parent:
            return
        close_hooks += 1
        if close_hooks == 1:
            raise OSError("injected first parent cleanup-after-effect")

    monkeypatch.setattr(economic_agent, "_path_race_hook", fail_first_parent_close)
    with pytest.raises(OSError, match="first parent cleanup-after-effect"):
        load_economic_observations(ledger)
    assert close_hooks == expected_closes


def test_ledger_lock_double_parent_swap_cannot_escape_or_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger_parent = tmp_path / "lock-parent"
    ledger_parent.mkdir()
    outside = tmp_path / "lock-outside"
    outside.mkdir()
    moved = tmp_path / "lock-original"
    swapped_parent: Path | None = None

    def swap(phase: str, parent: Path) -> None:
        nonlocal swapped_parent
        if parent != ledger_parent:
            return
        if phase == "ledger-lock-after-final-check" and swapped_parent is None:
            parent.rename(moved)
            _make_directory_alias(outside, parent)
            swapped_parent = parent
        elif phase == "ledger-lock-after-open" and swapped_parent is not None:
            _remove_directory_alias(parent)
            moved.rename(parent)
            swapped_parent = None

    monkeypatch.setattr(economic_agent, "_path_race_hook", swap)
    ledger = ledger_parent / "observations.jsonl"
    try:
        with pytest.raises((OSError, ValueError)):
            append_economic_observations(
                ledger,
                [_observation("growth_nowcast", 0.5, "index")],
            )
        assert not ledger.exists()
        assert list(outside.rglob("*")) == []
    finally:
        if swapped_parent is not None:
            _remove_directory_alias(swapped_parent)
            moved.rename(swapped_parent)


def test_ledger_ancestor_double_swap_cannot_escape_or_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tree = tmp_path / "ledger-tree"
    ledger_parent = tree / "inner"
    ledger_parent.mkdir(parents=True)
    outside = tmp_path / "ancestor-outside"
    (outside / "inner").mkdir(parents=True)
    moved = tmp_path / "ledger-tree-original"
    ledger = ledger_parent / "observations.jsonl"
    append_economic_observations(
        ledger,
        [_observation("growth_nowcast", 0.5, "index")],
    )
    before = ledger.read_bytes()
    swapped = False

    def swap(phase: str, parent: Path) -> None:
        nonlocal swapped
        if parent != ledger_parent:
            return
        if phase == "ledger-after-final-check" and not swapped:
            tree.rename(moved)
            _make_directory_alias(outside, tree)
            swapped = True
        elif phase == "ledger-after-install" and swapped:
            _remove_directory_alias(tree)
            moved.rename(tree)
            swapped = False

    monkeypatch.setattr(economic_agent, "_path_race_hook", swap)
    try:
        with pytest.raises((OSError, ValueError)):
            append_economic_observations(
                ledger,
                [_observation("inflation_nowcast", 3.0, "percent")],
            )
        assert ledger.read_bytes() == before
        assert [path for path in outside.rglob("*") if path.is_file()] == []
    finally:
        if swapped:
            _remove_directory_alias(tree)
            moved.rename(tree)



def test_ledger_paths_reject_hardlinks_and_copied_aliases(tmp_path: Path) -> None:
    ledger = tmp_path / "observations.jsonl"
    _fresh_observations(ledger)
    hardlink = tmp_path / "observations-hardlink.jsonl"
    os.link(ledger, hardlink)
    with pytest.raises(ValueError, match="硬链接"):
        load_economic_observations(ledger)
    with pytest.raises(ValueError, match="硬链接"):
        load_economic_observations(hardlink)
    hardlink.unlink()
    assert len(load_economic_observations(ledger)) == 6

    copied = tmp_path / "observations-copy.jsonl"
    shutil.copyfile(ledger, copied)
    with pytest.raises(ValueError, match="登记路径"):
        load_economic_observations(copied)


def test_agent_ledger_rejects_hardlink_aliases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation_ledger = tmp_path / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    policy = _policy()
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    output = tmp_path / "output"
    agent_ledger = tmp_path / "agent-runs.jsonl"
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    run_economic_research_agent(
        context=context,
        proposals=[_proposal(observations[0].observation_id)],
        policy=policy,
        observation_ledger_path=observation_ledger,
        output=output,
        ledger_path=agent_ledger,
    )
    alias = tmp_path / "agent-runs-hardlink.jsonl"
    os.link(agent_ledger, alias)
    for path in (agent_ledger, alias):
        with pytest.raises(ValueError, match="硬链接"):
            verify_economic_agent_ledger(
                path,
                observation_ledger_path=observation_ledger,
                output=output,
                policy=policy,
            )
    alias.unlink()
    assert len(verify_economic_agent_ledger(
        agent_ledger,
        observation_ledger_path=observation_ledger,
        output=output,
        policy=policy,
    )) == 1


def test_cli_confines_inputs_and_write_targets_to_root(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    source = root / "observation.json"
    source.write_text(
        canonical_json(_observation("growth_nowcast", 1.0, "index")),
        encoding="utf-8",
    )
    outside = tmp_path / "outside" / "observations.jsonl"
    with pytest.raises(ValueError, match="--root"):
        _economic_agent_main([
            "--root", str(root), "ingest", "--input", str(source),
            "--ledger", str(outside),
        ])
    outside_source = tmp_path / "outside-input.json"
    outside_source.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    with pytest.raises(ValueError, match="--root"):
        _economic_agent_main([
            "--root", str(root), "ingest", "--input", str(outside_source),
            "--ledger", "data/research/economic/observations.jsonl",
        ])
    with pytest.raises(ValueError, match="允许写入目录"):
        _economic_agent_main([
            "--root", str(root), "ingest", "--input", str(source),
            "--ledger", "config/observations.jsonl",
        ])
    with pytest.raises(ValueError, match="路径别名"):
        _economic_agent_main([
            "--root", str(root), "ingest", "--input", str(source),
            "--ledger", "data/research/economic/../observations.jsonl",
        ])
    assert not outside.exists()
    allowed = root / "data" / "research" / "economic"
    allowed.mkdir(parents=True)
    outside_directory = tmp_path / "outside-directory"
    outside_directory.mkdir()
    link = allowed / "escape"
    try:
        os.symlink(outside_directory, link, target_is_directory=True)
    except OSError:
        pass
    else:
        with pytest.raises(ValueError, match="符号链接|junction"):
            _economic_agent_main([
                "--root", str(root), "ingest", "--input", str(source),
                "--ledger", "data/research/economic/escape/observations.jsonl",
            ])


def test_cli_rejects_junction_at_allowed_output_root(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    source = root / "observation.json"
    source.write_text(
        canonical_json(_observation("growth_nowcast", 1.0, "index")),
        encoding="utf-8",
    )
    assert _economic_agent_main([
        "--root", str(root), "ingest", "--input", str(source),
    ]) == 0
    policy_path = root / "policy.json"
    policy_path.write_text(
        canonical_json(_policy().payload()) + "\n",
        encoding="utf-8",
    )
    reports = root / "reports"
    reports.mkdir()
    escaped = root / "config"
    escaped.mkdir()
    _make_directory_alias(escaped, reports / "economic-research")
    with pytest.raises(ValueError, match="junction|允许写入根"):
        _economic_agent_main([
            "--root", str(root), "context",
            "--policy", str(policy_path),
            "--decision-time", "2026-05-03T00:00:00Z",
        ])
    assert list(escaped.rglob("*.json")) == []


def test_cli_propose_and_verify_end_to_end(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    observation_input = root / "observation.json"
    observation_input.write_text(
        canonical_json(_observation("growth_nowcast", 1.0, "index")) + "\n",
        encoding="utf-8",
    )
    policy_path = root / "policy.json"
    policy_path.write_text(
        canonical_json(_policy().payload()) + "\n",
        encoding="utf-8",
    )
    assert _economic_agent_main([
        "--root", str(root), "ingest", "--input", str(observation_input),
    ]) == 0
    ingest_result = json.loads(capsys.readouterr().out)
    evidence_id = cast(list[str], ingest_result["observation_ids"])[0]

    assert _economic_agent_main([
        "--root", str(root), "context", "--policy", str(policy_path),
        "--decision-time", "2026-05-03T00:00:00Z",
    ]) == 0
    context_result = json.loads(capsys.readouterr().out)
    context_path = Path(cast(str, context_result["path"]))
    proposals_path = root / "proposals.jsonl"
    proposals_path.write_text(
        canonical_json(_proposal(evidence_id)) + "\n",
        encoding="utf-8",
    )
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    assert _economic_agent_main([
        "--root", str(root), "propose", "--context", str(context_path),
        "--proposals", str(proposals_path), "--policy", str(policy_path),
    ]) == 0
    propose_result = json.loads(capsys.readouterr().out)
    assert propose_result["accepted_proposal_ids"] == []
    assert propose_result["proposal_paths"] == []
    assert propose_result["rejected_count"] == 1
    assert propose_result["receipt_storage"] == "embedded_in_ledger"
    assert "receipt" not in propose_result
    assert Path(cast(str, propose_result["ledger"])).name == "agent-runs.jsonl"
    assert not (root / "reports" / "economic-research" / "runs").exists()

    assert _economic_agent_main([
        "--root", str(root), "verify", "--policy", str(policy_path),
    ]) == 0
    verify_result = json.loads(capsys.readouterr().out)
    assert verify_result == {
        "agent_run_count": 1,
        "command": "verify",
        "observation_count": 1,
        "research_only": True,
    }


def test_cli_proposal_count_and_byte_limits_fail_before_persistence(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    policy = _policy(max_proposals=2)
    policy_path = root / "policy.json"
    policy_path.write_text(canonical_json(policy.payload()) + "\n", encoding="utf-8")
    observation_ledger = (
        root / "data" / "research" / "economic" / "observations.jsonl"
    )
    observations = _fresh_observations(observation_ledger)
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        policy,
    )
    context_path = write_content_addressed_artifact(
        root / "reports" / "economic-research" / "contexts",
        context,
        "economic-context",
    )
    proposal = _proposal(observations[0].observation_id)
    proposals_path = root / "too-many.json"
    proposals_path.write_text(
        canonical_json([proposal, proposal, proposal]) + "\n",
        encoding="utf-8",
    )
    base_arguments = [
        "--root", str(root), "propose", "--context", str(context_path),
        "--policy", str(policy_path),
    ]
    with pytest.raises(ValueError, match="records 输入上限"):
        _economic_agent_main([
            *base_arguments,
            "--proposals", str(proposals_path),
        ])

    proposals_path.write_text(
        canonical_json(proposal) + " " * (2 * 64 * 1024 + 4097),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="bytes 输入上限"):
        _economic_agent_main([
            *base_arguments,
            "--proposals", str(proposals_path),
        ])
    assert not (root / "data" / "research" / "economic" / "agent-runs.jsonl").exists()
    assert not (root / "reports" / "economic-research" / "runs").exists()
