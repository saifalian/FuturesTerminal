from __future__ import annotations


def score_liquidation_pressure(events: int) -> float:
    return min(1.0, events / 20.0)
