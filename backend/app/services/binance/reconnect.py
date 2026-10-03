from __future__ import annotations

import random


class ReconnectPolicy:
    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 20.0,
        jitter_min: float = 0.85,
        jitter_max: float = 1.25,
    ) -> None:
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter_min = jitter_min
        self.jitter_max = jitter_max

    def delay(self, attempt: int) -> float:
        value = self.base_delay * (2 ** max(0, attempt - 1))
        value = min(value, self.max_delay)
        jitter = random.uniform(self.jitter_min, self.jitter_max)
        return min(self.max_delay, max(self.base_delay, value * jitter))
