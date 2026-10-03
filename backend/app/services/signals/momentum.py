from __future__ import annotations


def score_momentum(return_pct: float) -> float:
    return max(-1.0, min(1.0, return_pct / 2.0))
