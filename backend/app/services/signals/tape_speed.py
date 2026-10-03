from __future__ import annotations


def score_tape_speed(trades_per_sec: float) -> float:
    return min(1.0, trades_per_sec / 50.0)
