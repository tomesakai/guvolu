"""只读上游的策略研究管线。"""

from guvolu.research.pipeline import ResearchRunResult, run_research
from guvolu.research.verification import VerificationResult, verify_research_run

__all__ = [
    "ResearchRunResult",
    "VerificationResult",
    "run_research",
    "verify_research_run",
]
