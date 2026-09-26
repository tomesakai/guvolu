"""分档放量：由放量表推导信封草案与两市场风险预算（2026-09-26 快照）。

每档只给一个量：每市场风险预算 B。其余取值按 config/stage_scaling.json
的倍数规则推导（G-06）：单笔 = B（目标适配器要求预算不超过单笔硬顶），
单日 = 4B，持仓上限 = 1.5B，累计亏损熔断 = 0.55B（冻结计划研究最坏回撤
约 0.36B 的 1.5 倍），当日亏损停机 = 0.15B，信封总额 = 30B，首单压额
= 0.6B（两个冻结计划最大合计目标 0.6 与 0.577，首单不会被压额拒绝）。

describe 只读：列出各档推导值与现场门槛（硬顶、两仓 .env 限额、
两仓目标配置预算、最新实盘估值）。draft 写信封草案并把主仓两份目标
配置改为本档预算；签发仍由 issue_envelope.ps1 完成，执行仓在签发的
快进步骤里同时拿到新信封与新预算。本脚本不读写密钥、不发任何请求。
"""
from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from guvolu.domain.config import (
    MAX_DAY_COUNT_CEILING,
    MAX_DAY_JPY_CEILING,
    MAX_ORDER_JPY_CEILING,
    load_config,
)

REPO = Path(__file__).resolve().parents[1]
STAGE_TABLE = Path("config/stage_scaling.json")
ENVELOPE = Path("config/authorization_envelope.json")
TARGET_CONFIGS = ("config/paper_executor.json", "config/paper_executor_eth.json")
LIVE_REPORTS = Path("data/execution/live/reports")
DEFAULT_EXECUTION = Path("C:/Users/wu_zh/dev/guvolu-exec")
RULE_FIELDS = (
    "order_jpy_max", "day_jpy_max", "max_position_jpy",
    "max_cumulative_loss_jpy", "day_loss_jpy_max", "envelope_jpy_total",
    "canary_first_order_jpy_max",
)


@dataclass(frozen=True, slots=True)
class Stage:
    """放量表中的一档。"""

    name: str
    risk_budget_jpy: Decimal
    minimum_valuation_jpy: Decimal
    gate: str


@dataclass(frozen=True, slots=True)
class StageTable:
    """放量表：倍数规则、日笔数、有效天数与各档。"""

    rules: Mapping[str, Decimal]
    day_count_max: int
    validity_days: int
    stages: tuple[Stage, ...]

    def stage(self, name: str) -> Stage:
        for item in self.stages:
            if item.name == name:
                return item
        raise ValueError(f"放量表没有档位: {name}")


def load_stage_table(path: Path) -> StageTable:
    """装载并校验放量表；倍数缺项或非正即拒绝。"""
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("schema_version") != 1:
        raise ValueError("放量表 schema_version 不受支持")
    rules_raw = raw.get("rules")
    if not isinstance(rules_raw, dict) or set(rules_raw) != set(RULE_FIELDS):
        raise ValueError("放量表 rules 字段集合不符")
    rules = {key: Decimal(str(value)) for key, value in rules_raw.items()}
    if any(value <= 0 for value in rules.values()):
        raise ValueError("放量表倍数必须为正")
    if rules["order_jpy_max"] != 1:
        raise ValueError("单笔上限倍数必须为 1：预算不得超过单笔上限")
    stages = []
    for item in raw.get("stages") or []:
        stages.append(Stage(
            name=str(item["stage"]),
            risk_budget_jpy=Decimal(str(item["risk_budget_jpy"])),
            minimum_valuation_jpy=Decimal(str(item["minimum_valuation_jpy"])),
            gate=str(item.get("gate") or ""),
        ))
    if not stages:
        raise ValueError("放量表没有档位")
    day_count = int(raw["day_count_max"])
    validity = int(raw["validity_days"])
    if not 0 < day_count <= MAX_DAY_COUNT_CEILING or not 0 < validity <= 60:
        raise ValueError("放量表日笔数或有效天数越界")
    return StageTable(rules, day_count, validity, tuple(stages))


def derive_limits(table: StageTable, stage: Stage) -> dict[str, Decimal]:
    """按倍数规则推导信封金额字段，取整到日元。"""
    budget = stage.risk_budget_jpy
    return {
        field: (budget * multiple).quantize(Decimal("1"))
        for field, multiple in table.rules.items()
    }


def _latest_valuation(execution: Path) -> tuple[str, Decimal] | None:
    reports = sorted(
        (execution / LIVE_REPORTS).glob("*.json"),
        key=lambda path: path.stat().st_mtime,
    )
    for path in reversed(reports[-20:]):
        body = json.loads(path.read_text(encoding="utf-8"))
        value = body.get("valuation_after_jpy")
        if value is not None:
            return str(body.get("generated_at")), Decimal(str(value))
    return None


def _budget(path: Path) -> Decimal:
    return Decimal(str(json.loads(path.read_text(encoding="utf-8"))["risk_budget_jpy"]))


def assess(
    table: StageTable, stage: Stage, *, repository: Path, execution: Path,
) -> tuple[dict[str, object], list[str]]:
    """列出本档推导值与现场门槛；返回摘要与阻断原因。"""
    limits = derive_limits(table, stage)
    blockers: list[str] = []
    if limits["order_jpy_max"] > MAX_ORDER_JPY_CEILING:
        blockers.append(f"单笔 {limits['order_jpy_max']} 超硬顶 {MAX_ORDER_JPY_CEILING}")
    if limits["day_jpy_max"] > MAX_DAY_JPY_CEILING:
        blockers.append(f"单日 {limits['day_jpy_max']} 超硬顶 {MAX_DAY_JPY_CEILING}")
    env_limits: dict[str, dict[str, str]] = {}
    for label, root in (("main", repository), ("exec", execution)):
        config = load_config(root / ".env")
        env_limits[label] = {
            "order_jpy_max": format(config.limits.order_jpy_max, "f"),
            "day_jpy_max": format(config.limits.day_jpy_max, "f"),
            "day_count_max": str(config.limits.day_count_max),
        }
        if config.limits.order_jpy_max < limits["order_jpy_max"]:
            blockers.append(f"{label} .env 单笔限额 {config.limits.order_jpy_max} 低于本档")
        if config.limits.day_jpy_max < limits["day_jpy_max"]:
            blockers.append(f"{label} .env 单日限额 {config.limits.day_jpy_max} 低于本档")
        if config.limits.day_count_max < table.day_count_max:
            blockers.append(f"{label} .env 日笔数 {config.limits.day_count_max} 低于本档")
    budgets = {
        f"{label}:{name}": format(_budget(root / name), "f")
        for label, root in (("main", repository), ("exec", execution))
        for name in TARGET_CONFIGS
    }
    valuation = _latest_valuation(execution)
    if valuation is None:
        blockers.append("没有可读的实盘估值")
    elif valuation[1] < stage.minimum_valuation_jpy:
        blockers.append(
            f"最新估值 {valuation[1]} 低于本档门槛 {stage.minimum_valuation_jpy}"
        )
    summary: dict[str, object] = {
        "stage": stage.name,
        "risk_budget_jpy": format(stage.risk_budget_jpy, "f"),
        "gate": stage.gate,
        "envelope_limits": {key: format(value, "f") for key, value in limits.items()},
        "day_count_max": table.day_count_max,
        "validity_days": table.validity_days,
        "ceilings": {
            "order": format(MAX_ORDER_JPY_CEILING, "f"),
            "day": format(MAX_DAY_JPY_CEILING, "f"),
            "count": MAX_DAY_COUNT_CEILING,
        },
        "env_limits": env_limits,
        "target_budgets": budgets,
        "latest_valuation": None if valuation is None else {
            "at": valuation[0], "valuation_jpy": format(valuation[1], "f"),
        },
        "minimum_valuation_jpy": format(stage.minimum_valuation_jpy, "f"),
        "blockers": blockers,
    }
    return summary, blockers


def build_draft(
    current: Mapping[str, object],
    table: StageTable,
    stage: Stage,
    *,
    valid_from: datetime,
) -> dict[str, object]:
    """以现行信封为模板生成草案：只改金额、日笔数与有效期。"""
    limits = derive_limits(table, stage)
    draft = dict(current)
    stamp = valid_from.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    until = (valid_from + timedelta(days=table.validity_days)).astimezone(UTC)
    draft["issued_at"] = stamp
    draft["valid_from"] = stamp
    draft["valid_until"] = until.strftime("%Y-%m-%dT%H:%M:%SZ")
    draft["day_count_max"] = table.day_count_max
    for field, value in limits.items():
        draft[field] = format(value, "f")
    return draft


def _write_budget(path: Path, budget: Decimal) -> bool:
    body = json.loads(path.read_text(encoding="utf-8"))
    text = format(budget, "f")
    if body.get("risk_budget_jpy") == text:
        return False
    body["risk_budget_jpy"] = text
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return True


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="分档放量：信封草案与风险预算")
    parser.add_argument("--repository", type=Path, default=REPO)
    parser.add_argument("--execution-repository", type=Path, default=DEFAULT_EXECUTION)
    sub = parser.add_subparsers(dest="action", required=True)
    describe = sub.add_parser("describe", help="只读列出推导值与门槛")
    describe.add_argument("--stage", default=None)
    draft = sub.add_parser("draft", help="写信封草案并改主仓目标配置预算")
    draft.add_argument("--stage", required=True)
    draft.add_argument("--output", type=Path, required=True)
    draft.add_argument("--valid-from", default=None, help="ISO8601 UTC；缺省为当前整点")
    draft.add_argument(
        "--allow-low-valuation", action="store_true",
        help="估值未达本档门槛仍写草案（入金在途时预先准备）",
    )
    args = parser.parse_args(argv)
    repository = args.repository.resolve()
    execution = args.execution_repository.resolve()
    table = load_stage_table(repository / STAGE_TABLE)
    if args.action == "describe":
        names = [args.stage] if args.stage else [item.name for item in table.stages]
        summaries = [
            assess(table, table.stage(name), repository=repository, execution=execution)[0]
            for name in names
        ]
        print(json.dumps(summaries, ensure_ascii=False, indent=2))
        return 0
    stage = table.stage(args.stage)
    summary, blockers = assess(table, stage, repository=repository, execution=execution)
    if args.allow_low_valuation:
        blockers = [item for item in blockers if not item.startswith("最新估值")]
    if blockers:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        raise SystemExit("本档门槛未满足: " + "；".join(blockers))
    if args.valid_from:
        valid_from = datetime.fromisoformat(args.valid_from.replace("Z", "+00:00"))
    else:
        valid_from = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    current = json.loads((repository / ENVELOPE).read_text(encoding="utf-8"))
    body = build_draft(current, table, stage, valid_from=valid_from)
    output = args.output if args.output.is_absolute() else repository / args.output
    if output.exists():
        raise SystemExit(f"草案已存在，拒绝覆盖: {output}")
    output.write_text(json.dumps(body, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    changed = [
        name for name in TARGET_CONFIGS
        if _write_budget(repository / name, stage.risk_budget_jpy)
    ]
    print(json.dumps({
        "stage": stage.name,
        "draft": str(output),
        "target_configs_changed": changed,
        "next": [
            "git add config/ && git commit",
            f"issue_envelope.ps1 -Draft {output.relative_to(repository)}（每小时 :45 至 :10 之间）",
        ],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
