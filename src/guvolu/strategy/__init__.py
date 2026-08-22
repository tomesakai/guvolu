"""无网络和委托能力的策略纯函数。"""

from guvolu.strategy.baselines import build_candidates, generate_targets
from guvolu.strategy.contracts import CandidateSpec, FeatureRow, ResearchBar
from guvolu.strategy.generation import (
    FamilyCandidateBatch,
    build_family_batches,
    candidate_search_plan_payload,
)
from guvolu.strategy.search_plan import evaluate_search_plan_candidate
from guvolu.strategy.mutation import (
    StructuralChallenger,
    bounded_typed_crossovers,
    bounded_typed_mutations,
)

__all__ = [
    "CandidateSpec",
    "FamilyCandidateBatch",
    "FeatureRow",
    "ResearchBar",
    "StructuralChallenger",
    "bounded_typed_crossovers",
    "bounded_typed_mutations",
    "build_candidates",
    "build_family_batches",
    "candidate_search_plan_payload",
    "evaluate_search_plan_candidate",
    "generate_targets",
]
