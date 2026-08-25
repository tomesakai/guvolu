"""Research-only 经济代理的 PIT、台账与提案门禁测试。"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from guvolu.research import economic_agent
from guvolu.research.economic_agent import (
    append_economic_observations,
    build_economic_context,
    load_content_addressed_artifact,
    load_economic_observation_snapshot,
    load_economic_observations,
    parse_economic_policy,
    run_economic_research_agent,
    verify_economic_context,
    verify_economic_agent_ledger,
    write_content_addressed_artifact,
)
from guvolu.research.provenance import canonical_json, stable_identifier


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _policy(
    holdout: str | None = None,
):
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
            "max_proposals_per_run": 2,
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
    monkeypatch.setattr(economic_agent.clock, "utc_now", lambda: timestamp)


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


def _fresh_observations(path: Path):
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
    assert first.observation_id in replay["selected_observation_ids"]
    assert replay["quality"]["pit"] is True


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
    assert observation.observation_id in context["selected_observation_ids"]
    assert context["quality"]["pit_basis"] == (
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
    assert observation.observation_id not in before["selected_observation_ids"]
    assert observation.observation_id in after["selected_observation_ids"]


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


def test_agent_outputs_proposal_only_and_audits_duplicates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """接受制品无配置/注册/促销权限，重复仍必须入账。"""
    observation_ledger = tmp_path / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        _policy(),
    )
    evidence_id = observations[0].observation_id
    agent_ledger = tmp_path / "agent-runs.jsonl"
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    result = run_economic_research_agent(
        context=context,
        proposals=[_proposal(evidence_id)],
        policy=_policy(),
        observation_ledger_path=observation_ledger,
        output=tmp_path / "output",
        ledger_path=agent_ledger,
    )
    assert len(result.proposal_paths) == 1 and result.rejected_count == 0
    artifact = load_content_addressed_artifact(
        result.proposal_paths[0], "economic-search-plan-proposal",
    )
    interface = artifact["search_plan_interface"]
    assert interface["contract"] == "proposal_only"
    assert interface["may_write_config"] is False
    assert interface["may_write_registry"] is False
    assert interface["may_promote"] is False
    assert interface["holdout_governance_bound"] is False
    assert artifact["authority"]["trade"] is False
    receipt = load_content_addressed_artifact(
        result.receipt_path, "economic-agent-run",
    )
    assert receipt["model_identity"]["provider"] == "none"
    assert receipt["prompt_identity"]["prompt_template_id"] == "none"

    _freeze_clock(monkeypatch, "2026-05-03T00:02:00Z")
    duplicate = run_economic_research_agent(
        context=context,
        proposals=[_proposal(evidence_id)],
        policy=_policy(),
        observation_ledger_path=observation_ledger,
        output=tmp_path / "output",
        ledger_path=agent_ledger,
    )
    assert duplicate.proposal_paths == () and duplicate.rejected_count == 1
    rows = verify_economic_agent_ledger(agent_ledger)
    assert len(rows) == 2
    assert rows[1]["attempts"][0]["reasons"] == ["duplicate_proposal"]


def test_agent_rejects_unknown_evidence_wrong_regime_quota_and_holdout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation_ledger = tmp_path / "observations.jsonl"
    observations = _fresh_observations(observation_ledger)
    context = build_economic_context(
        load_economic_observation_snapshot(observation_ledger),
        _time("2026-05-03T00:00:00Z"),
        _policy(),
    )
    invalid = _proposal("economic-observation-" + "0" * 64)
    invalid["regimes"] = ["growth:weak"]
    first = _proposal(observations[0].observation_id, entry=1.0)
    second = _proposal(observations[0].observation_id, entry=2.0)
    _freeze_clock(monkeypatch, "2026-05-03T00:01:00Z")
    result = run_economic_research_agent(
        context=context,
        proposals=[invalid, first, second],
        policy=_policy(),
        observation_ledger_path=observation_ledger,
        output=tmp_path / "output",
        ledger_path=tmp_path / "agent-runs.jsonl",
    )
    assert len(result.proposal_paths) == 2
    assert result.rejected_count == 1
    rows = verify_economic_agent_ledger(tmp_path / "agent-runs.jsonl")
    reasons = rows[0]["attempts"][0]["reasons"]
    assert "evidence_not_fresh_or_not_in_context" in reasons
    assert "regime_not_supported_by_current_context" in reasons

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
    holdout_rows = verify_economic_agent_ledger(tmp_path / "holdout-runs.jsonl")
    assert "holdout_boundary_reached" in holdout_rows[0]["attempts"][0]["reasons"]
    assert "holdout_governance_unbound" in holdout_rows[0]["attempts"][0]["reasons"]


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
    receipt = load_content_addressed_artifact(
        before_boundary.receipt_path, "economic-agent-run",
    )
    assert receipt["authority"]["holdout_governance_bound"] is False
    assert receipt["input_identity"]["holdout_governance"] == {
        "binding": None,
        "bound": False,
        "reason": "holdout_governance_unbound",
    }
    assert receipt["attempts"][0]["reasons"] == [
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
    rows = verify_economic_agent_ledger(ledger)
    assert "holdout_governance_unbound" in rows[1]["attempts"][0]["reasons"]
    assert "holdout_boundary_reached" in rows[1]["attempts"][0]["reasons"]


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
