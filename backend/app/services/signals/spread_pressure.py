from __future__ import annotations


def score_spread_pressure(best_bid: float, best_ask: float) -> float:
    if best_bid <= 0 or best_ask <= 0:
        return 0.0
    spread = best_ask - best_bid
    mid = (best_ask + best_bid) / 2
    if mid == 0:
        return 0.0
    return max(0.0, 1.0 - spread / mid * 1_000)
