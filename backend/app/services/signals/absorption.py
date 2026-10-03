from __future__ import annotations


def score_absorption(volume: float, move_ticks: float) -> float:
    if volume <= 0:
        return 0.0
    return min(1.0, volume / max(1.0, move_ticks) / 100.0)
