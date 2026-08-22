"""生成停在代码注册前边界的有界 typed 结构 challenger。"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.config_lineage import load_governed_strategy_config
from guvolu.research.provenance import canonical_json, sha256_text
from guvolu.strategy.expression import strategy_expression
from guvolu.strategy.generation import (
    GENERATOR_METHOD_VERSION,
    build_family_batches,
    candidate_search_plan_payload,
)
from guvolu.strategy.mutation import (
    MUTATION_OPERATORS,
    bounded_typed_crossovers,
    bounded_typed_mutations,
    structural_challenger_registry_payload,
)


def main(argv: Sequence[str] | None = None) -> int:
    """生成一个流派的未注册结构 challenger 制品。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("family")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/strategy_research.json"),
    )
    parser.add_argument(
        "--operator",
        action="append",
        choices=MUTATION_OPERATORS,
        dest="operators",
    )
    parser.add_argument("--donor-family", action="append", dest="donors")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args(argv)
    root = arguments.root.resolve()
    config_path = arguments.config
    if not config_path.is_absolute():
        config_path = root / config_path
    config, config_hash, _lineage_root_hash, _lineage_depth = (
        load_governed_strategy_config(root, config_path)
    )
    batch = build_family_batches(config, (arguments.family,))[0]
    maximum_structures = batch.candidate_budget // len(batch.candidates) - 1
    requested_limit = (
        maximum_structures if arguments.limit is None else arguments.limit
    )
    if requested_limit < 0:
        raise ValueError("limit 不得为负")
    limit = min(requested_limit, maximum_structures)
    template = strategy_expression(arguments.family)
    challengers = list(bounded_typed_mutations(
        template,
        MUTATION_OPERATORS if arguments.operators is None else arguments.operators,
        limit,
    ))
    remaining = limit - len(challengers)
    for donor_family in sorted(set(arguments.donors or ())):
        if remaining <= 0:
            break
        donor = strategy_expression(donor_family)
        additions = bounded_typed_crossovers(template, donor, remaining)
        known = {item.expression_id for item in challengers}
        challengers.extend(
            item for item in additions if item.expression_id not in known
        )
        remaining = limit - len(challengers)
    payload = structural_challenger_registry_payload(
        arguments.family,
        config_hash,
        [candidate.candidate_id for candidate in batch.candidates],
        str(candidate_search_plan_payload((batch,))["search_plan_id"]),
        GENERATOR_METHOD_VERSION,
        batch.candidate_budget,
        challengers[:limit],
    )
    content = canonical_json(payload) + "\n"
    digest = sha256_text(content)
    output = arguments.output
    if output is None:
        output = (
            root / "reports" / "strategy-research"
            / "structural-challengers" / arguments.family
        )
    elif not output.is_absolute():
        output = root / output
    path = output / f"structural-challengers-sha256-{digest}.json"
    atomic_write_text(path, content)
    print(canonical_json({
        "path": path.as_posix(),
        "sha256": digest,
        "family": arguments.family,
        "status": payload["status"],
        "structural_challenger_count": payload["structural_challenger_count"],
        "projected_candidate_count": payload["projected_candidate_count"],
        "candidate_budget": payload["candidate_budget"],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
