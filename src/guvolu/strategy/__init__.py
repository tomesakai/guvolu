"""无网络和委托能力的策略纯函数。"""

from guvolu.strategy.baselines import build_candidates, generate_targets
from guvolu.strategy.contracts import CandidateSpec, FeatureRow, ResearchBar
from guvolu.strategy.generation import (
    FamilyCandidateBatch,
    build_family_batches,
    candidate_search_plan_payload,
)
from guvolu.strategy.search_plan import evaluate_search_plan_candidate

__all__ = [
    "CandidateSpec",
    "FamilyCandidateBatch",
    "FeatureRow",
    "ResearchBar",
    "build_candidates",
    "build_family_batches",
    "candidate_search_plan_payload",
    "evaluate_search_plan_candidate",
    "generate_targets",
]
