from __future__ import annotations

from app.core.math_utils import clamp
from app.core.models import BiasOutput


def combine_scores(weights: dict[str, float], raw: dict[str, float]) -> BiasOutput:
    long_score = 0.0
    short_score = 0.0
    reason_tags: list[str] = []

    for key, value in raw.items():
        weighted = value * weights.get(key, 1.0)
        if weighted > 0:
            long_score += weighted
        elif weighted < 0:
            short_score += abs(weighted)
        if abs(weighted) > 0.25:
            reason_tags.append(key)

    total = long_score + short_score
    confidence = clamp(total / 10.0, 0.0, 1.0)
    danger_score = clamp(abs(long_score - short_score) / 10.0, 0.0, 1.0)

    return BiasOutput(
        long_score=long_score,
        short_score=short_score,
        confidence=confidence,
        danger_score=danger_score,
        reason_tags=reason_tags,
    )
