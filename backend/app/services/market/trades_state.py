from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field


@dataclass(slots=True)
class TradesState:
    maxlen: int = 2000
    trades: deque[dict] = field(default_factory=lambda: deque(maxlen=2000))

    def add(self, trade: dict) -> None:
        self.trades.append(trade)
