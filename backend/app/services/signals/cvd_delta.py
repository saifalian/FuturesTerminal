from __future__ import annotations


def score_cvd_delta(cvd: float) -> float:
    return max(-1.0, min(1.0, cvd / 1000.0))
