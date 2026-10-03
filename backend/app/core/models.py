from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class BiasOutput:
    long_score: float = 0.0
    short_score: float = 0.0
    confidence: float = 0.0
    danger_score: float = 0.0
    reason_tags: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RawEvent:
    stream: str
    event_type: str
    payload: dict[str, Any]
    received_at: str
