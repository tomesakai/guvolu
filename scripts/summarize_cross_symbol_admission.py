"""多品种联合准入汇总：把若干研究运行的家族结论并排成一张表。

输入为各品种研究运行目录（含 summary.json），输出 Markdown 表与
JSON。只读，不改写任何运行制品，不触碰交易端点（G-01）。
统一口径：同一回撤上限、同一基准超额下限对全部品种一视同仁，
逐家族给出「统计门」「回撤门」「基准超额门」三项判定与合计。
阈值全部来自命令行参数（G-06），不在代码内固化。
"""
from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class FamilyRow:
    """一个品种一个家族的联合准入行。"""

    market_id: str
    run_id: str
    family: str
    sharpe: float
    drawdown: float
    deployment_drawdown: float | None
    fdr_q: float
    pbo: float
    dsr_effective: float
    block_p: float
    positive_fold_ratio: float
    benchmark_sharpe: float | None
    benchmark_excess: float | None
    eligible_in_run: bool
    rejection_reasons: tuple[str, ...]
    statistical_gate: bool
    drawdown_gate: bool
    benchmark_gate: bool | None
    joint: bool


def _number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _optional_number(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    return float(value) if isinstance(value, (int, float)) else None


def load_rows(
    run_directory: Path,
    *,
    maximum_drawdown: float,
    minimum_benchmark_excess: float,
    maximum_pbo: float,
    maximum_fdr_q: float,
    minimum_dsr: float,
    maximum_block_p: float,
) -> list[FamilyRow]:
    """读取一个运行的 summary.json 并按统一阈值判定。"""
    summary_path = run_directory / "summary.json"
    body = json.loads(summary_path.read_text(encoding="utf-8"))
    if not isinstance(body, Mapping):
        raise ValueError(f"{summary_path} 根必须为对象")
    market_id = str(body.get("market_id"))
    run_id = str(body.get("run_id"))
    ablations = body.get("ablations")
    benchmark_sharpe: float | None = None
    if isinstance(ablations, Mapping):
        fixed_long = ablations.get("fixed_long")
        if isinstance(fixed_long, Mapping):
            benchmark_sharpe = _optional_number(fixed_long.get("sharpe"))
    evaluations = body.get("family_evaluations")
    if not isinstance(evaluations, list):
        raise ValueError(f"{summary_path} 缺 family_evaluations")
    rows: list[FamilyRow] = []
    for item in evaluations:
        if not isinstance(item, Mapping):
            continue
        metrics = item.get("metrics")
        if not isinstance(metrics, Mapping):
            continue
        deployment = item.get("deployment_oos_metrics")
        deployment_drawdown = (
            _optional_number(deployment.get("maximum_drawdown"))
            if isinstance(deployment, Mapping) else None
        )
        sharpe = _number(metrics.get("sharpe"), "sharpe")
        drawdown = _number(metrics.get("maximum_drawdown"), "maximum_drawdown")
        fdr_q = _number(item.get("fdr_q"), "fdr_q")
        pbo = _number(
            item.get("probability_backtest_overfitting"),
            "probability_backtest_overfitting",
        )
        dsr = _number(
            item.get("deflated_sharpe_probability_effective"),
            "deflated_sharpe_probability_effective",
        )
        block_p = _number(
            item.get("block_bootstrap_p_value"), "block_bootstrap_p_value"
        )
        fold_ratio = _number(
            item.get("positive_fold_ratio"), "positive_fold_ratio"
        )
        excess = (
            None if benchmark_sharpe is None else sharpe - benchmark_sharpe
        )
        statistical = (
            fdr_q <= maximum_fdr_q
            and pbo <= maximum_pbo
            and dsr >= minimum_dsr
            and block_p <= maximum_block_p
            and fold_ratio >= 0.5
        )
        # 回撤门同时看验证序列与部署候选
        drawdown_gate = drawdown <= maximum_drawdown and (
            deployment_drawdown is None
            or deployment_drawdown <= maximum_drawdown
        )
        benchmark_gate = (
            None if excess is None else excess >= minimum_benchmark_excess
        )
        reasons = item.get("rejection_reasons")
        reason_tuple = tuple(
            str(reason) for reason in reasons
        ) if isinstance(reasons, list) else ()
        rows.append(FamilyRow(
            market_id=market_id,
            run_id=run_id,
            family=str(item.get("family")),
            sharpe=sharpe,
            drawdown=drawdown,
            deployment_drawdown=deployment_drawdown,
            fdr_q=fdr_q,
            pbo=pbo,
            dsr_effective=dsr,
            block_p=block_p,
            positive_fold_ratio=fold_ratio,
            benchmark_sharpe=benchmark_sharpe,
            benchmark_excess=excess,
            eligible_in_run=bool(item.get("eligible")),
            rejection_reasons=reason_tuple,
            statistical_gate=statistical,
            drawdown_gate=drawdown_gate,
            benchmark_gate=benchmark_gate,
            joint=statistical and drawdown_gate and benchmark_gate is not False,
        ))
    return rows


def _fmt(value: float | None, digits: int = 3) -> str:
    return "" if value is None else f"{value:.{digits}f}"


def render_markdown(rows: Sequence[FamilyRow]) -> str:
    """按家族分组输出并排表。"""
    lines = [
        "| 家族 | 品种 | 运行 | Sharpe | 回撤 | 部署回撤 | FDR q | PBO | DSR | Block p | 基准超额 | 运行准入 | 统计门 | 回撤门 | 基准门 | 联合 |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|",
    ]
    mark = {True: "可", False: "不可", None: "无基准"}
    for row in sorted(rows, key=lambda r: (r.family, r.market_id)):
        lines.append(
            f"| {row.family} | {row.market_id} | {row.run_id[13:25]} | "
            f"{_fmt(row.sharpe)} | {_fmt(row.drawdown)} | "
            f"{_fmt(row.deployment_drawdown)} | {_fmt(row.fdr_q, 4)} | "
            f"{_fmt(row.pbo)} | {_fmt(row.dsr_effective)} | "
            f"{_fmt(row.block_p, 4)} | {_fmt(row.benchmark_excess)} | "
            f"{mark[row.eligible_in_run]} | {mark[row.statistical_gate]} | "
            f"{mark[row.drawdown_gate]} | {mark[row.benchmark_gate]} | "
            f"{mark[row.joint]} |"
        )
    families = sorted({row.family for row in rows})
    lines.append("")
    lines.append("| 家族 | 联合通过品种数 | 参评品种数 |")
    lines.append("|---|---:|---:|")
    for family in families:
        members = [row for row in rows if row.family == family]
        lines.append(
            f"| {family} | {sum(1 for row in members if row.joint)} | "
            f"{len(members)} |"
        )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="多品种联合准入汇总（只读）")
    parser.add_argument("runs", nargs="+", type=Path, help="研究运行目录")
    parser.add_argument("--maximum-drawdown", type=float, default=0.45)
    parser.add_argument("--minimum-benchmark-excess", type=float, default=0.05)
    parser.add_argument("--maximum-pbo", type=float, default=0.4)
    parser.add_argument("--maximum-fdr-q", type=float, default=0.2)
    parser.add_argument("--minimum-dsr", type=float, default=0.95)
    parser.add_argument("--maximum-block-p", type=float, default=0.05)
    parser.add_argument("--output", type=Path, default=None, help="JSON 输出路径")
    args = parser.parse_args(argv)
    rows: list[FamilyRow] = []
    for run in args.runs:
        rows.extend(load_rows(
            run,
            maximum_drawdown=args.maximum_drawdown,
            minimum_benchmark_excess=args.minimum_benchmark_excess,
            maximum_pbo=args.maximum_pbo,
            maximum_fdr_q=args.maximum_fdr_q,
            minimum_dsr=args.minimum_dsr,
            maximum_block_p=args.maximum_block_p,
        ))
    print(render_markdown(rows))
    if args.output is not None:
        payload = {
            "schema_version": 1,
            "kind": "cross_symbol_admission_summary",
            "thresholds": {
                "maximum_drawdown": args.maximum_drawdown,
                "minimum_benchmark_excess": args.minimum_benchmark_excess,
                "maximum_pbo": args.maximum_pbo,
                "maximum_fdr_q": args.maximum_fdr_q,
                "minimum_dsr": args.minimum_dsr,
                "maximum_block_p": args.maximum_block_p,
            },
            "rows": [asdict(row) for row in rows],
        }
        args.output.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
