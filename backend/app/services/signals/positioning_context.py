from __future__ import annotations


def score_positioning_context(oi_change_pct: float) -> float:
    return max(-1.0, min(1.0, oi_change_pct / 10.0))
