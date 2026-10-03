from __future__ import annotations


def score_spoofing(canceled_large_levels: int) -> float:
    return min(1.0, canceled_large_levels / 10.0)
