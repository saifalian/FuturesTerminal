from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field


@dataclass(slots=True)
class LiquidationState:
    maxlen: int = 500
    events: deque[dict] = field(default_factory=lambda: deque(maxlen=500))

    def add(self, payload: dict) -> None:
        self.events.append(payload)
